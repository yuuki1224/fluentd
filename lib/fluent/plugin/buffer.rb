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

require 'fluent/plugin/feature'
require 'monitor'

module Fluent
  module Plugin
    class Buffer < Feature
      include Fluent::MonitorMixin

      class BufferError < StandardError; end
      class BufferOverflowError < BufferError; end
      class BufferChunkOverflowError < BufferError; end # A record size is larger than chunk size limit

      MINIMUM_APPEND_ATTEMPT_RECORDS = 10

      DEFAULT_CHUNK_BYTES_LIMIT =   8 * 1024 * 1024 # 8MB
      DEFAULT_TOTAL_BYTES_LIMIT = 512 * 1024 * 1024 # 512MB, same with v0.12 (BufferedOutput + buf_memory: 64 x 8MB)

      # TODO: system total buffer bytes limit by SystemConfig

      config_param :chunk_bytes_limit, :size, default: DEFAULT_CHUNK_BYTES_LIMIT
      config_param :total_bytes_limit, :size, default: DEFAULT_TOTAL_BYTES_LIMIT

      # If user specify this value and (chunk_size * queue_length) is smaller than total_size,
      # then total_size is automatically configured to that value
      config_param :queue_length_limit, :integer, default: nil

      # optional new limitations
      config_param :chunk_records_limit, :integer, default: nil

      attr_accessor :log

      Metadata = Struct.new(:timekey, :tag, :variables)

      def initialize
        super

        @chunk_bytes_limit = nil
        @total_bytes_limit = nil
        @queue_length_limit = nil
        @chunk_records_limit = nil

        @log = nil

        @_owner = nil
        @_plugin_id = nil
        @_plugin_id_configured = false
      end

      def persistent?
        false
      end

      def configure(conf)
        super

        if @queue_length_limit && @total_bytes_limit > @chunk_bytes_limit * @queue_length_limit
          @total_bytes_limit = @chunk_bytes_limit * @queue_length_limit
        end
      end

      def start
        super
        # stage #=> Hash (metadata -> chunk): not flushed yet
        # queue #=> Array (chunks)          : already flushed (not written)
        @stage, @queue = resume

        @queued_num = {} # metadata => int (number of queued chunks)
        @queue.each do |chunk|
          @queued_num[chunk.metadata] ||= 0
          @queued_num[chunk.metadata] += 1
        end

        @dequeued = {} # unique_id => chunk

        @stage_size = @queue_size = 0
        @metadata_list = [] # keys of @stage
      end

      def stop
        # ...
      end

      def shutdown
        # ...
      end

      def close
        synchronize do
          @dequeued.each_pair do |chunk_id, chunk|
            chunk.close
          end
          until @queue.empty?
            @queue.shift.close
          end
          @stage.each_pair do |metadata, chunk|
            chunk.close
          end
        end
      end

      def terminate
        @stage = @queue = nil
      end

      def storable?
        @total_size_limit > @stage_size + @queue_size
      end

      ## TODO: for back pressure feature
      # def used?(ratio)
      #   @total_size_limit * ratio > @stage_size + @queue_size
      # end

      def resume
        # return {}, []
        raise NotImplementedError, "Implement this method in child class"
      end

      def generate_chunk(metadata)
        raise NotImplementedError, "Implement this method in child class"
      end

      def metadata_list
        synchronize do
          @metadata_list.dup
        end
      end

      def new_metadata(timekey: nil, tag: nil, variables: [])
        Metadata.new(timekey, tag, variables)
      end

      def add_metadata(metadata)
        synchronize do
          if i = @metadata_list.index(metadata)
            @metadata_list[i]
          else
            @metadata_list << metadata
          end
        end
      end

      def metadata(timekey: nil, tag: nil, key_value_pairs: {})
        meta = if key_value_pairs.empty?
                 new_metadata(timekey: timekey, tag: tag, variables: [])
               else
                 variables = key_value_pairs.keys.sort.map{|k| key_value_pairs[k] }
                 new_meatadata(timekey: timekey, tag: tag, variables: variables)
               end
        add_metadata(meta)
        meta
      end

      # metadata MUST have consistent object_id for each variation
      # data MUST be Array of serialized events
      def emit(metadata, data force: false)
        data_size = data.size
        return if data_size < 1
        raise BufferOverflowError unless storable?

        stored = false

        # the case whole data can be stored in staged chunk: almost all emits will success
        chunk = synchronize { @stage[metadata] ||= generate_chunk(metadata) }
        chunk.synchronize do
          begin
            chunk.append(data)
            if !size_over?(chunk) || force
              chunk.commit
              stored = true
            end
          ensure
            chunk.rollback
          end
        end
        return if stored

        # try step-by-step appending if data can't be stored into existing a chunk
        emit_step_by_step(metadata, data)
      end

      # def staged_chunk_test(metadata) # block
      #   synchronize do
      #     chunk = @stage[metadata]
      #     yield chunk
      #   end
      # end
      # def queued_records
      #   synchronize { @queue.reduce(0){|r, chunk| r + chunk.records } }
      # end

      def queued?(metadata=nil)
        synchronize do
          return !@queue.empty? unless metadata
          n = @queued_num[metadata]
          n && n.nonzero?
        end
      end

      def enqueue_chunk(metadata)
        synchronize do
          chunk = @stage.delete(metadata)
          if chunk
            chunk.synchronize do
              if chunk.empty?
                chunk.close
              else
                @queue << chunk
                @queued_num[metadata] = @queued_num.fetch(metadata, 0) + 1
                chunk.enqueued! if chunk.respond_to?(:enqueued!)
              end
            end
          end
          nil
        end
      end

      def enqueue_all
        synchronize do
          @stage.keys.each do |metadata|
            enqueue_chunk(metadata)
          end
        end
      end

      def dequeue_chunk
        return nil if @queue.empty?
        synchronize do
          chunk = @queue.shift
          return nil unless chunk # queue is empty
          @dequeued[chunk.unique_id] = chunk
          @queued_num[chunk.metadata] -= 1 # BUG if nil, 0 or subzero
          chunk
        end
      end

      def takeback_chunk(chunk_id)
        synchronize do
          chunk = @dequeued.delete(chunk_id)
          return false unless chunk # already purged by other thread
          @queue.unshift(chunk)
          @queued_num[chunk.metadata] += 1 # BUG if nil
          true
        end
      end

      def purge_chunk(chunk_id)
        synchronize do
          chunk = @dequeued.delete(chunk_id)
          metadata = chunk && chunk.metadata

          begin
            chunk.purge if chunk
          rescue => e
            @log.error "failed to purge buffer chunk", chunk_id: chunk_id, error_class: e.class, error: e
          end

          if metadata && !@stage[metadata] && (!@queued_num[metadata] || @queued_num[metadata] < 1)
            @metadata_list.delete(metadata)
          end
        end
        nil
      end

      def clear!
        synchronize do
          until @queue.empty?
            begin
              @queue.shift.purge
            rescue => e
              @log.error "unexpected error while clearing buffer queue", error_class: e.class, error: e
            end
          end
        end
      end

      def size_over?(chunk)
        chunk.size > @chunk_bytes_limit || (@chunk_records_limit && chunk.records > @chunk_records_limit)
      end

      def emit_step_by_step(metadata, data)
        attempt_records = data.size / 3

        synchronize do # critical section for buffer (stage/queue)
          while data.size > 0
            if attempt_records < MINIMUM_APPEND_ATTEMPT_RECORDS
              attempt_records = MINIMUM_APPEND_ATTEMPT_RECORDS
            end

            chunk = @stage[metadata]
            unless chunk
              chunk = @stage[metadata] = generate_chunk(metadata)
            end

            chunk.synchronize do # critical section for chunk (chunk append/commit/rollback)
              begin
                empty_chunk = chunk.empty?

                attempt = data.slice(0, attempt_records)
                chunk.append(attempt)

                if size_over?(chunk)
                  chunk.rollback

                  if attempt_records <= MINIMUM_APPEND_ATTEMPT_RECORDS
                    if empty_chunk # record is too large even for empty chunk
                      raise BufferChunkOverflowError, "minimum append butch exceeds chunk bytes limit"
                    end
                    # no more records for this chunk -> enqueue -> to be flushed
                    enqueue_chunk(metadata) # `chunk` will be removed from stage
                    attempt_records = data.size # fresh chunk may have enough space
                  else
                    # whole data can be processed by twice operation
                    #  ( by using apttempt /= 2, 3 operations required for odd numbers of data)
                    attempt_records = (attempt_records / 2) + 1
                  end

                  next
                end

                chunk.commit
                data.slice!(0, attempt_records)
                # same attempt size
                nil # discard return value of data.slice!() immediately
              ensure
                chunk.rollback
              end
            end
          end
        end
        nil
      end # emit_step_by_step
    end
  end
end
