#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

require 'fluent/plugin/base'
require 'fluent/log'
require 'fluent/plugin_id'
require 'fluent/plugin_helper'
require 'fluent/timezone'

require 'time'
require 'monitor'

module Fluent
  module Plugin
    class Output < Base
      include PluginId
      include PluginLoggerMixin
      include PluginHelper::Mixin

      helpers :thread, :retry_state

      CHUNK_KEY_PATTERN = /^[-_.@a-zA-Z0-9]+$/
      CHUNK_KEY_PLACEHOLDER_PATTERN = /\$\{[-_.@a-zA-Z0-9]+\}/

      # `<buffer>` and `<secondary>` sections are available only when '#format' and '#write' are implemented
      config_section :buffer, param_name: :buffer_config, init: true, required: false, multi: false, final: true do
        config_argument :chunk_keys, :array, value_type: :string, default: []
        config_param :@type, :string, default: 'memory2'

        config_param :timekey_range, :time, default: nil # range size to be used: `time.to_i / @timekey_range`
        config_param :timekey_use_utc, :bool, default: false # default is localtime
        config_param :timekey_zone, :string, default: Time.now.strftime('%z') # '+0900'
        config_param :timekey_wait, :time, default: 600

        desc 'If true, plugin will try to flush buffer just before shutdown.'
        config_param :flush_at_shutdown, :bool, default: nil # change default by buffer_plugin.persistent?

        desc 'How to enqueue chunks to be flushed. "fast" flushes per flush_interval, "immediate" flushes just after event arrival.'
        config_param :flush_mode, :enum, list: [:default, :none, :fast, :immediate], default: :default
        config_param :flush_interval, :time, default: 60, desc: 'The interval between buffer chunk flushes.'

        config_param :flush_threads, :integer, default: 1, desc: 'The number of threads to flush the buffer.'

        config_param :flush_thread_interval, :float, default: 1.0, desc: 'Seconds to sleep between checks for buffer flushes in flush threads.'
        config_param :flush_burst_interval, :float, default: 1.0, desc: 'Seconds to sleep between flushes when many buffer chunks are queued.'

        config_param :delayed_commit_timeout, :time, default: 60, desc: 'Seconds of timeout for buffer chunks to be committed by plugins later.'

        config_param :retry_forever, :bool, default: false, desc: 'If true, plugin will ignore retry_timeout and retry_max_times options and retry flushing forever.'
        config_param :retry_timeout, :time, default: 72 * 60 * 60, desc: 'The maximum seconds to retry to flush while failing, until plugin discards buffer chunks.'
        # 72hours == 17 times with exponential backoff (not to change default behavior)
        config_param :retry_max_times, :integer, default: nil, desc: 'The maximum number of times to retry to flush while failing.'

        config_param :retry_secondary_threshold, :integer, default: 80, desc: 'Percentage of retry_timeout to switch to use secondary while failing.'
        # expornential backoff sequence will be initialized at the time of this threshold

        desc 'How to wait next retry to flush buffer.'
        config_param :retry_type, :enum, list: [:expbackoff, :periodic], default: :expbackoff
        ### Periodic -> fixed :retry_wait
        ### Exponencial backoff: k is number of retry times
        # c: constant factor, @retry_wait
        # b: base factor, @retry_backoff_base
        # k: times
        # total retry time: c + c * b^1 + (...) + c*b^k = c*b^(k+1) - 1
        config_param :retry_wait, :time, default: 1, desc: 'Seconds to wait before next retry to flush, or constant factor of exponential backoff.'
        config_param :retry_backoff_base, :float, default: 2, desc: 'The base number of exponencial backoff for retries.'
        config_param :retry_max_interval, :time, default: nil, desc: 'The maximum interval seconds for exponencial backoff between retries while failing.'
      end

      config_section :secondary, param_name: :secondary_config, required: false, multi: false, final: true do
        config_param :@type, :string, default: nil
        config_section :buffer, required: false, multi: false do
          # dummy to detect invalid specification for here
        end
      end

      def process(tag, es)
        raise NotImplementedError, "BUG: output plugins MUST implement this method"
      end

      def format(tag, time, record)
        raise NotImplementedError, "BUG: output plugins MUST implement this method"
      end

      def write(chunk)
        raise NotImplementedError, "BUG: output plugins MUST implement this method"
      end

      def try_write(chunk)
        raise NotImplementedError, "BUG: output plugins MUST implement this method"
      end

      def prefer_buffered_processing
        # override this method to return false only when all of these are true:
        #  * plugin has both implementation for buffered and non-buffered methods
        #  * plugin is expected to work as non-buffered plugin if no `<buffer>` sections specified
        true
      end

      def prefer_delayed_commit
        # override this method to decide which is used of `write` or `try_write` if both are implemented
        true
      end

      # Internal states
      FlushThreadState = Struct.new(:thread, :next_time)
      DequeuedChunkInfo = Struct.new(:chunk_id, :time)

      attr_reader :num_errors, :emit_count, :emit_records, :write_count, :rollback_count

      # for tests
      attr_reader :buffer, :chunk_keys, :chunk_key_time, :chunk_key_tag
      attr_accessor :output_enqueue_thread_waiting

      def initialize
        super
        @buffering = false
        @delayed_commit = false
      end

      def configure(conf)
        unless implement?(:synchronous) || implement?(:buffered) || implement?(:delayed_commit)
          raise "BUG: output plugin must implement some methods. see developer documents."
        end

        has_buffer_section = (conf.elements.select{|e| e.name == 'buffer' }.size > 0)

        super

        if has_buffer_section
          unless implement?(:buffered) || implement?(:delayed_commit)
            raise Fluent::ConfigError, "<buffer> section is configured, but plugin '#{self.class}' doesn't support buffering"
          end
          @buffering = true
        else # no buffer sections
          if implement?(:synchronous)
            if !implement?(:buffered) && !implement?(:delayed_commit)
              @buffering = false
            else
              # @buffering.nil? shows that enabling buffering or not will be decided in lazy way in #start
              @buffering = nil
            end
          else # buffered or delayed_commit is supported by `unless` of first line in this method
            @buffering = true
          end
        end

        if @buffering || @buffering.nil?
          # When @buffering.nil?, @buffer_config was initialized with default value for all parameters.
          # If so, this configuration MUST success.
          @chunk_keys = @buffer_config.chunk_keys
          @chunk_key_time = !!@chunk_keys.delete('time')
          @chunk_key_tag = !!@chunk_keys.delete('tag')
          if @chunk_keys.any?{ |key| key !~ CHUNK_KEY_PATTERN }
            raise Fluent::ConfigError, "chunk_keys specification includes invalid char"
          end

          if @chunk_key_time
            raise Fluent::ConfigError, "<buffer ...> argument includes 'time', but timekey_range is not configured" unless @buffer_config.timekey_range
            Fluent::Timezone.validate!(@buffer_config.timekey_zone)
            @buffer_config.timekey_zone = '+0000' if @buffer_config.timekey_use_utc
            @output_time_formatter_cache = {}
          end

          @flush_mode = @buffer_config.flush_mode
          if @flush_mode == :default
            @flush_mode = (@chunk_key_time ? :none : :fast)
          end

          buffer_type = @buffer_config[:@type]
          buffer_conf = conf.elements.select{|e| e.name == 'buffer' }.first || Fluent::Config::Element.new('buffer', '', {}, [])
          @buffer = Plugin.new_buffer(buffer_type, parent: self)
          @buffer.configure(buffer_conf)

          @flush_at_shutdown = @buffer_config.flush_at_shutdown
          if @flush_at_shutdown.nil?
            @flush_at_shutdown = if @buffer.persistent?
                                   false
                                 else
                                   true # flush_at_shutdown is true in default for on-memory buffer
                                 end
          elsif !@flush_at_shutdown && !@buffer.persistent?
            buf_type = Plugin.lookup_type_from_class(@buffer.class)
            log.warn "'flush_at_shutdown' is false, and buffer plugin '#{buf_type}' is not persistent buffer."
            log.warn "your configuration will lose buffered data at shutdown. please confirm your configuration again."
          end
        end

        if @secondary_config
          raise Fluent::ConfigError, "Invalid <secondary> section for non-buffered plugin" unless @buffering
          raise Fluent::ConfigError, "<secondary> section cannot have <buffer> section" if @secondary_config.buffer
          raise Fluent::ConfigError, "<secondary> section and 'retry_forever' are exclusive" if @buffer_config.retry_forever

          secondary_type = @secondary_config[:@type]
          secondary_conf = conf.elements.select{|e| e.name == 'secondary' }.first
          @secondary = Plugin.new_output(secondary_type)
          @secondary.configure(secondary_conf)
          @secondary.router = router if @secondary.has_router?
          if self.class != @secondary.class
            log.warn "secondary type should be same with primary one", primary: self.class.to_s, secondary: @secondary.class.to_s
          end
        else
          @secondary = nil
        end

        self
      end

      def start
        super
        # TODO: well organized counters
        @counters_monitor = Monitor.new
        @num_errors = 0
        @emit_count = 0
        @emit_records = 0
        @write_count = 0
        @rollback_count = 0

        if @buffering.nil?
          @buffering = prefer_buffered_processing
          unless @buffering
            @buffer.terminate # it's not started, so terminate will be enough
          end
        end

        if @buffering
          m = method(:emit_buffered)
          (class << self; self; end).module_eval do
            define_method(:emit, m)
          end

          @delayed_commit = if implement?(:buffered) && implement?(:delayed_commit)
                              prefer_delayed_commit
                            else
                              implement?(:delayed_commit)
                            end

          @retry = nil
          @retry_mutex = Mutex.new

          @buffer.start

          @output_flush_threads = []
          @output_flush_threads_mutex = Mutex.new
          @output_flush_threads_running = true

          # mainly for test: detect enqueue works as code below:
          #   @output.interrupt_flushes
          #   # emits
          #   @output.enqueue_thread_wait
          @output_flush_interrupted = false
          @output_enqueue_thread_mutex = Mutex.new
          @output_enqueue_thread_waiting = false

          @dequeued_chunks = []
          @dequeued_chunks_mutex = Mutex.new

          @buffer_config.flush_threads.times do |i|
            thread_title = "flush_thread_#{i}".to_sym
            thread_state = FlushThreadState.new(nil, nil)
            thread = thread_create(thread_title, thread_state, &method(:flush_thread_run))
            thread_state.thread = thread
            @output_flush_threads_mutex.synchronize do
              @output_flush_threads << thread_state
            end
          end
          @output_flush_thread_current_position = 0

          if @flush_mode == :fast || @chunk_key_time
            thread_create(:enqueue_thread, &method(:enqueue_thread_run))
          end
        else # !@buffered
          m = method(:emit_sync)
          (class << self; self; end).module_eval do
            define_method(:emit, m)
          end
        end
      end

      def stop
        super
        @buffer.stop if @buffering
      end

      def before_shutdown
        super

        if @buffering
          if @flush_at_shutdown
            force_flush
          end
          @buffer.before_shutdown
        end
      end

      def shutdown
        super
        @buffer.shutdown if @buffering
      end

      def after_shutdown
        super
        try_rollback_all if @delayed_commit # for delayed chunks

        if @buffering
          @buffer.after_shutdown

          @output_flush_threads_running = false
          @output_flush_threads.each do |state|
            state.thread.run if state.thread.alive? # to wakeup thread and make it to stop by itself
          end
          @output_flush_threads.each do |state|
            state.thread.join
          end
        end
      end

      def close
        super
        @buffer.close if @buffering
      end

      def terminate
        super
        @buffer.terminate if @buffering
      end

      def support_in_v12_style?(feature)
        # for plugins written in v0.12 styles
        case feature
        when :synchronous    then false
        when :buffered       then false
        when :delayed_commit then false
        end
      end

      def implement?(feature)
        methods_of_plugin = self.class.instance_methods(false)
        case feature
        when :synchronous    then methods_of_plugin.include?(:process) || support_in_v12_style?(:synchronous)
        when :buffered       then methods_of_plugin.include?(:format) && methods_of_plugin.include?(:write) || support_in_v12_style?(:buffered)
        when :delayed_commit then methods_of_plugin.include?(:format) && methods_of_plugin.include?(:try_write)
        else
          raise ArgumentError, "Unknown feature for output plugin: #{feature}"
        end
      end

      # TODO: optimize this code
      def extract_placeholders(str, metadata)
        if metadata.timekey.nil? && metadata.tag.nil? && metadata.variables.nil?
          str
        else
          rvalue = str
          # strftime formatting
          if @chunk_key_time # this section MUST be earlier than rest to use raw 'str'
            @output_time_formatter_cache[str] ||= Fluent::Timezone.formatter(@buffer_config.timekey_zone, str)
            rvalue = @output_time_formatter_cache[str].call(metadata.timekey)
          end
          # ${tag}, ${tag[0]}, ${tag[1]}, ...
          if @chunk_key_tag
            if str =~ /\$\{tag\[\d+\]\}/
              hash = {'${tag}' => metadata.tag}
              metadata.tag.split('.').each_with_index do |part, i|
                hash["${tag[#{i}]}"] = part
              end
              rvalue = rvalue.gsub(/\$\{tag(\[\d+\])?\}/, hash)
            elsif str.include?('${tag}')
              rvalue = rvalue.gsub('${tag}', metadata.tag)
            end
          end
          # ${a_chunk_key}, ...
          if !@chunk_keys.empty? && metadata.variables
            hash = {'${tag}' => '${tag}'} # not to erase this wrongly
            @chunk_keys.each do |key|
              hash["${#{key}}"] = metadata.variables[key.to_sym]
            end
            rvalue = rvalue.gsub(CHUNK_KEY_PLACEHOLDER_PATTERN, hash)
          end
          rvalue
        end
      end

      def emit(tag, es)
        # actually this method will be overwritten by #configure
        if @buffering
          emit_buffered(tag, es)
        else
          emit_sync(tag, es)
        end
      end

      def emit_sync(tag, es)
        @counters_monitor.synchronize{ @emit_count += 1 }
        begin
          process(tag, es)
          # TODO: how to count records of es? add API to event streams?
          # @counters_monitor.synchronize{ @emit_records += records }
        rescue
          @counters_monitor.synchronize{ @num_errors += 1 }
          raise
        end
      end

      def emit_buffered(tag, es)
        @counters_monitor.synchronize{ @emit_count += 1 }
        begin
          metalist = handle_stream(tag, es)
          if @flush_mode == :immediate
            metalist.each do |meta|
              @buffer.enqueue_chunk(meta)
            end
          end
          if !@retry && @buffer.queued?
            submit_flush_once
          end
        rescue
          # TODO: separate number of errors into emit errors and write/flush errors
          @counters_monitor.synchronize{ @num_errors += 1 }
          raise
        end
      end

      # TODO: optimize this code
      def metadata(tag, time, record)
        timekey_range = @buffer_config.timekey_range
        if @chunk_keys.empty?
          if !@chunk_key_time && !@chunk_key_tag
            @buffer.metadata()
          elsif @chunk_key_time && @chunk_key_tag
            time_int = time.to_i
            timekey = time_int - (time_int % timekey_range)
            @buffer.metadata(timekey: timekey, tag: tag)
          elsif @chunk_key_time
            time_int = time.to_i
            timekey = time_int - (time_int % timekey_range)
            @buffer.metadata(timekey: timekey)
          else
            @buffer.metadata(tag: tag)
          end
        else
          timekey = if @chunk_key_time
                      time_int = time.to_i
                      time_int - (time_int % timekey_range)
                    else
                      nil
                    end
          pairs = Hash[@chunk_keys.map{|k| [k.to_sym, record[k]]}]
          @buffer.metadata(timekey: timekey, tag: (@chunk_key_tag ? tag : nil), variables: pairs)
        end
      end

      def handle_stream(tag, es)
        meta_and_data = {}
        records = 0
        es.each do |time, record|
          meta = metadata(tag, time, record)
          meta_and_data[meta] ||= []
          meta_and_data[meta] << format(tag, time, record)
          records += 1
        end
        meta_and_data.each_pair do |meta, data|
          @buffer.emit(meta, data)
        end
        @counters_monitor.synchronize{ @emit_records += records }
        meta_and_data.keys
      end

      def commit_write(chunk_id)
        if @delayed_commit
          @dequeued_chunks_mutex.synchronize do
            @dequeued_chunks.delete_if { |info| info.chunk_id == chunk_id }
          end
        end
        @buffer.purge_chunk(chunk_id)

        @retry_mutex.synchronize do
          if @retry # success to flush chunks in retries
            log.warn "retry succeeded.", plugin_id: plugin_id, chunk_id: chunk_id
            @retry = nil
            @counters_monitor.synchronize{ @num_errors = 0 }
          end
        end
      end

      def try_rollback_write
        now = current_time
        @dequeued_chunks_mutex.synchronize do
          while @dequeued_chunks.first && @dequeued_chunks.first.time + @buffer_config.delayed_commit_timeout < now
            info = @dequeued_chunks.shift
            if @buffer.takeback_chunk(info.chunk_id)
              # TODO: @rollback_count
              log.warn "failed to flush chunk, timeout to commit.", plugin_id: plugin_id, chunk_id: info.chunk_id, flushed_at: info.time
              log.warn_backtrace e.backtrace
            end
          end
        end
      end

      def try_rollback_all
        @dequeued_chunks_mutex.synchronize do
          until @dequeued_chunks.empty?
            info = @dequeued_chunks.shift
            if @buffer.takeback_chunk(info.chunk_id)
              # TODO: @rollback_count
              log.info "delayed commit for buffer chunk was cancelled in shutdown", plugin_id: plugin_id, chunk_id: info.chunk_id
            end
          end
        end
      end

      def next_flush_time
        if @buffer.queued?
          @retry_mutex.synchronize do
            @retry ? @retry.next_time : current_time + @buffer_config.flush_burst_interval
          end
        else
          current_time + @buffer_config.flush_thread_interval
        end
      end

      def try_flush
        chunk = @buffer.dequeue_chunk
        p({c: !!chunk})
        return unless chunk

        output = self
        using_secondary = false
        if @retry_mutex.synchronize{ @retry && @retry.secondary? }
          output = @secondary
          using_secondary = true
        end

        begin
          if @delayed_commit && output.implement?(:delayed_commit) # output may be secondary, and secondary plugin may not support delayed commit
            output.try_write(chunk)
            @counters_monitor.synchronize{ @write_count += 1 }
            @dequeued_chunks_mutex.synchronize do
              @dequeued_chunks << DequeuedChunkInfo.new(chunk.unique_id, current_time)
            end
          else # output plugin without delayed purge
            chunk_id = chunk.unique_id
            p "write"
            output.write(chunk)
            p "countup"
            @counters_monitor.synchronize{ @write_count += 1 }
            p "commit"
            commit_write(chunk_id)
          end
        rescue => e
          @retry_mutex.synchronize do
            if @retry
              @retry.step
              @counters_monitor.synchronize{ @num_errors += 1 }
              if @retry.limit?
                records = @buffer.queued_records
                log.error "failed to flush the buffer, and hit limit for retries. dropping all chunks in buffer queue.", plugin_id: plugin_id, records: records, error_class: e.class, error: e
                log.error_backtrace e.backtrace
                @buffer.clear!
                @retry = nil
              else
                log.warn "failed to flush the buffer#{using_secondary ? ' with secondary output' : ''}.", plugin_id: plugin_id, next_retry: @retry.next_time, error_class: e.class, error: e
                log.warn_backtrace e.backtrace
              end
            else
              raise
              p e.class
              p e.message
              p e
              @retry = retry_state()
              @counters_monitor.synchronize{ @num_errors += 1 }
              log.warn "failed to flush the buffer.", plugin_id: plugin_id, next_retry: @retry.next_time, error_class: e.class, error: e
              log.warn_backtrace e.backtrace
            end
          end
        end
      end

      def retry_state
        if @secondary
          retry_state_create(
            :output_retries, @buffer_config.retry_type, @buffer_config.retry_wait, @buffer_config.retry_timeout,
            forever: @buffer_config.retry_forever, max_steps: @buffer_config.retry_max_times, backoff_base: @buffer_config.retry_backoff_base,
            max_interval: @buffer_config.retry_max_interval,
            secondary: true, secondary_threshold: @buffer_config.retry_secondary_threshold
          )
        else
          retry_state_create(
            :output_retries, @buffer_config.retry_type, @buffer_config.retry_wait, @buffer_config.retry_timeout,
            forever: @buffer_config.retry_forever, max_steps: @buffer_config.retry_max_times, backoff_base: @buffer_config.retry_backoff_base,
            max_interval: @buffer_config.retry_max_interval
          )
        end
      end

      def submit_flush_once
        # Without locks: it is rough but enough to select "next" writer selection
        @output_flush_thread_current_position = (@output_flush_thread_current_position + 1) % @buffer_config.flush_threads
        state = @output_flush_threads[@output_flush_thread_current_position]
        state.next_time = 0
        state.thread.run
      end

      def force_flush
        @buffer.enqueue_all
        submit_flush_all
      end

      def submit_flush_all
        while !@retry && @buffer.queued?
          submit_flush_once
          sleep @buffer_config.flush_burst_interval
        end
      end

      # only for tests of output plugin
      def interrupt_flushes
        @output_flush_interrupted = true
      end

      # only for tests of output plugin
      def enqueue_thread_wait
        @output_enqueue_thread_mutex.synchronize do
          @output_flush_interrupted = false
          @output_enqueue_thread_waiting = true
        end
        require 'timeout'
        Timeout.timeout(10) do
          Thread.pass while @output_enqueue_thread_waiting
        end
        @output_flush_threads.each do |state|
          state.next_time = 0
          state.thread.run
        end
      end

      # only for tests of output plugin
      def flush_thread_wakeup
      end

      # to override in tests
      def current_time
        Time.now
      end

      def enqueue_thread_run
        value_for_interval = nil
        if @flush_mode == :fast
          value_for_interval = @buffer_config.flush_interval
        end
        if @chunk_key_time
          if !value_for_interval || @buffer_config.timekey_range < value_for_interval
            value_for_interval = @buffer_config.timekey_range
          end
        end
        unless value_for_interval
          raise "BUG: both of flush_interval and timekey are disabled"
        end
        interval = value_for_interval / 11.0
        if interval < @buffer_config.flush_thread_interval
          interval = @buffer_config.flush_thread_interval
        end

        begin
          while @output_flush_threads_running
            now = current_time
            if @output_flush_interrupted
              sleep interval
              next
            end

            @output_enqueue_thread_mutex.lock
            begin
              if @flush_mode == :fast
                flush_interval = @buffer_config.flush_interval
                @buffer.enqueue_all{ |metadata, chunk| chunk.created_at + flush_interval <= now }
              end

              if @chunk_key_time
                timekey_range = @buffer_config.timekey_range
                timekey_wait = @buffer_config.timekey_wait
                current_time_int = now.to_i
                current_time_range = current_time_int - current_time_int % timekey_range
                @buffer.enqueue_all{ |metadata, chunk| metadata.timekey < current_time_range && metadata.timekey + timekey_range + timekey_wait <= current_time_int }
              end
            rescue => e
              log.error "unexpected error while checking flushed chunks. ignored.", plugin_id: plugin_id, error_class: e.class, error: e
              log.error_backtrace
            end
            @output_enqueue_thread_waiting = false
            @output_enqueue_thread_mutex.unlock
            sleep interval
          end
        rescue => e
          # normal errors are rescued by inner begin-rescue clause.
          log.error "error on enqueue thread", plugin_id: plugin_id, error_class: e.class, error: e
          log.error_backtrace
          raise
        end
      end

      def flush_thread_run(state)
        flush_thread_interval = @buffer_config.flush_thread_interval

        # If the given clock_id is not supported, Errno::EINVAL is raised.
        clock_id = Process::CLOCK_MONOTONIC rescue Process::CLOCK_MONOTONIC_RAW
        state.next_time = Process.clock_gettime(clock_id) + flush_thread_interval

        begin
          # This thread don't use `thread_current_running?` because this thread should run in `before_shutdown` phase
          while @output_flush_threads_running
            time = Process.clock_gettime(clock_id)
            interval = state.next_time - time

            if state.next_time <= time && !@output_flush_interrupted
              p "try_flush"
              try_flush
              # next_flush_interval uses flush_thread_interval or flush_burst_interval (or retrying)
              interval = next_flush_time.to_f - current_time.to_f
              state.next_time = Process.clock_gettime(clock_id) + interval
            end

            if @delayed_commit && @dequeued_chunks_mutex.synchronize{ !@dequeued_chunks.empty? }
              unless @output_flush_interrupted
                try_rollback_write
              end
            end

            sleep interval if interval > 0
          end
        rescue => e
          raise
          # normal errors are rescued by output plugins in #try_flush
          # so this rescue section is for critical & unrecoverable errors
          log.error "error on output thread", plugin_id: plugin_id, error_class: e.class, error: e
          log.error_backtrace
          raise
        end
      end
    end
  end
end
