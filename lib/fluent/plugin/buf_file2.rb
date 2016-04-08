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

require 'fileutils'

require 'fluent/buffer'
require 'fluent/env'
require 'fluent/plugin'
require 'fluent/system_config'

module Fluent
  module Plugin
    class FileBuffer < Fluent::Plugin::Buffer
      Plugin.register_buffer('file2', self)

      include SystemConfig::Mixin

      DEFAULT_CHUNK_BYTES_LIMIT = 256 * 1024 * 1024        # 256MB
      DEFAULT_TOTAL_BYTES_LIMIT =  64 * 1024 * 1024 * 1024 #  64GB, same with v0.12 (TimeSlicedOutput + buf_file)

      # TODO: buffer_path based on system config
      desc 'The path where buffer chunks are stored.'
      config_param :path, :string, :permission

      config_set_default :chunk_bytes_limit, DEFAULT_CHUNK_BYTES_LIMIT
      config_set_default :total_bytes_limit, DEFAULT_TOTAL_BYTES_LIMIT

      config_param :file_permission, :string, default: nil # '0644'
      config_param :dir_permission,  :string, default: nil # '0755'

      ##TODO: Buffer plugin cannot handle symlinks because new API @stage has many writing buffer chunks
      ##      re-implement this feature on out_file, w/ enqueue_chunk(or generate_chunk) hook + chunk.path
      # attr_accessor :symlink_path

      @@buffer_paths = {}

      def initialize
        super
        @symlink_path = nil
      end

      def configure(conf)
        super

        type_of_owner = Plugin.lookup_type_from_class(@_owner.class)
        if @@buffer_paths.has_key?(@path)
          type_using_this_path = Plugin.lookup_type_from_class(@@buffer_paths[@path])
          raise ConfigError, "Other '#{type_using_this_path}' plugin already use same buffer path: type = #{type_of_owner}, buffer path = #{@path}"
        end

        @@buffer_paths[@path] = type_of_owner

        unless @path.include?('*')
          @path += '.*.log'
        end

        unless @dir_permission
          @dir_permission = system_config.dir_permission || DIR_PERMISSION
        end
      end

      def start
        FileUtils.mkdir_p File.dirname(@path), mode: @dir_permission

        super
      end

      def persistent?
        true
      end

      def resume
        stage = {}
        queue = []

        Dir.glob(@path) do |path|
          m = new_metadata() # this metadata will be overwritten by resuming .meta file content
                             # so it should not added into @metadata_list for now
          mode = Fluent::Plugin::Buffer::FileChunk.assume_chunk_state(path)
          chunk = Fluent::Plugin::Buffer::FileChunk.new(m, path, mode) # file chunk resumes contents of metadata
          case chunk.state
          when :staged
            stage[chunk.metadata] = chunk
          when :queued, :blocked
            queue << chunk
          else
            raise "BUG: unexpected chunk state '#{chunk.state}' for path '#{path}'"
          end
        end

        queue.sort_by!{ |chunk| chunk.modified_at }

        return stage, queue
      end

      def generate_chunk(metadata)
        # FileChunk generates real path with unique_id
        if @file_permission
          Fluent::Plugin::Buffer::FileChunk.new(metadata, @path, :create, perm: @file_permission)
        else
          Fluent::Plugin::Buffer::FileChunk.new(metadata, @path, :create)
        end
      end
    end
  end
end
