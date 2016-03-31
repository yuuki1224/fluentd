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

module Fluent
  module Plugin
    class Output < Base

      config_section :buffer, param_name: :buf_config, required: false, multi: false, final: true do
        config_argument(:chunk_keys, default: nil){ v.start_with?("[") ? JSON.load(v) : v.to_s.strip.split(/\s*,\s*/) } # TODO: use string_list
        
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

      def configure(conf)
        super
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
        if self.class.instance_methods.include?(:process)
        end
      end
    end
  end
end
