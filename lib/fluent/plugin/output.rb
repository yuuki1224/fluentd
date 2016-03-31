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
require 'fluent/plugin_helper/thread'

module Fluent
  module Plugin
    class Output < Base
      helpers :thread

      # `<buffer>` and `<secondary>` sections are available only when '#format' and '#write' are implemented
      # TODO: add `init: true` after merge of #877
      config_section :buffer, param_name: :buf_config, required: false, multi: false, final: true do
        config_argument(:chunk_keys, default: nil){ v.start_with?("[") ? JSON.load(v) : v.to_s.strip.split(/\s*,\s*/) } # TODO: use string_list
        config_param :@type, :string, default: 'memory'

        desc 'If true, plugin will try to flush buffer just before shutdown.'
        config_param :flush_at_shutdown, :bool, default: nil # change default by buffer_plugin.persistent?

        config_param :flush_interval, :time, default: 60, desc: 'The interval between buffer flushes.'
        config_param :flush_threads, :integer, default: 1, desc: 'The number of threads to flush the buffer.'
        config_param :flush_immediately, :bool, default: false, desc: 'If true, plugin will try to flush buffer immediately after events arrive.'

        config_param :flush_tread_interval, :float, default: 1.0, desc: 'Seconds to sleep between checks for buffer flushes in flush threads.'
        config_param :flush_burst_interval, :float, default: 1.0, desc: 'Seconds to sleep between flushes when many buffer chunks are queued.'

        config_param :retry_forever, :bool, default: false, desc: 'If true, plugin will ignore retry_limit_* options and retry flushing forever.'
        config_param :retry_limit_time,  :time, default: 72 * 60 * 60, desc: 'The maximum seconds to retry to flush while failing, until plugin discards buffer chunks.'
        # 72hours == 17 times with exponential backoff (not to change default behavior)
        config_param :retry_limit_times, :integer, default: nil, desc: 'The maximum number of times to retry to flush while failing.'

        config_param :retry_secondary_threshold, :integer, default: 80, desc: 'Percentage of retry_limit_* to switch to use secondary while failing.'
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

      def initialize
        super
        @buffering = false
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
            if !implement?(:buffered) && !implement(:delayed_commit)
              @buffering = false
            else
              @buffering = prefer_buffered_processing
            end
          else # buffered or delayed_commit is supported by `unless` of first line in this method
            @buffering = true
          end
        end

        if @buffering
          # TODO: configure buffering parameters and buffer plugin

          m = method(:emit_buffered)
          (class << self; self; end).module_eval do
            define_method(:emit, m)
          end
        else
          m = method(:emit_sync)
          (class << self; self; end).module_eval do
            define_method(:emit, m)
          end
        end

        if @secondary_config
          raise Fluent::ConfigError, "Invalid <secondary> section for non-buffered plugin" unless @buffering
          raise Fluent::ConfigError, "<secondary> section cannot have <buffer> section" if @secondary_config.buffer
          ### TODO: init/configure secondary plugin
        end

        self
      end

      def start
        super
        # start @buffer
        # start threads if @buffering
      end

      def stop
        super
        # stop @buffer
      end

      def shutdown
        super
        # shutdown @buffer
      end

      def close
        super
        # close @buffer
      end

      def terminate
        super
        # terminate @buffer
      end

      def implement?(feature)
        methods_of_plugin = self.class.instance_methods(false)
        case feature
        when :synchronous    then methods_of_plugin.include?(:process)
        when :buffered       then methods_of_plugin.include?(:format) && methods_of_plugin.include?(:write)
        when :delayed_commit then methods_of_plugin.include?(:format) && methods_of_plugin.include?(:try_write)
        else
          raise ArgumentError, "Unknown feature for output plugin: #{feature}"
        end
      end

      def emit_sync(tag, es)
        process(tag, es)
      end

      def emit_buffered(tag, es)
        # TODO: create hash of metadata => [formatted_lines]

        meta = metadata(tag)
        @emit_count += 1
        data = format_stream(tag, es)
        @buffer.emit(meta, data)
        [meta]
      end

      def emit(tag, es)
        # actually this method will be overwritten by #configure
        if @buffering
          emit_buffered(tag, es)
        else
          emit_sync(tag, es)
        end
      end

      def flush_thread_run
        # If the given clock_id is not supported, Errno::EINVAL is raised.
        clock_id = Process::CLOCK_MONOTONIC rescue Process::CLOCK_MONOTONIC_RAW
        next_time = Process.clock_gettime(clock_id) + 1.0

        begin
          until thread_current_running?
            time = Process.clock_gettime(clock_id)
            interval = @next_time - time

              if @delayed_purge_enabled && @dequeued_chunks_mutex.synchronize{ !@dequeued_chunks.empty? }
                @output.try_rollback_write
              end

              if @next_time < time
                @output.try_flush
                interval = @output.next_flush_time.to_f - Time.now.to_f
                @next_time = Process.clock_gettime(clock_id) + interval
              end

              sleep interval
            end
          rescue => e
            # normal errors are rescued by output plugins in #try_flush
            # so this rescue section is for critical & unrecoverable errors
            @log.error "error on output thread", error_class: e.class.to_s, error: e.to_s
            @log.error_backtrace
            raise
          end
      end
    end
  end
end
