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

require 'fluent/plugin'
require 'fluent/configurable'
require 'fluent/system_config'

module Fluent
  module Plugin
    class Base
      include Configurable
      include SystemConfig::Mixin

      State = Struct.new(:configure, :start, :stop, :before_shutdown, :shutdown, :after_shutdown, :close, :terminate)

      def initialize
        super
        @_state = State.new(false, false, false, false, false, false, false, false)
      end

      def has_router?
        false
      end

      def configure(conf)
        super
        @_state ||= State.new(false, false, false, false, false, false, false, false)
        @_state.configure = true
        self
      end

      def start
        @_state.start = true
        self
      end

      def stop
        @_state.stop = true
        self
      end

      def before_shutdown
        @_state.before_shutdown = true
        self
      end

      def shutdown
        @_state.shutdown = true
        self
      end

      def after_shutdown
        @_state.after_shutdown = true
        self
      end

      def close
        @_state.close = true
        self
      end

      def terminate
        @_state.terminate = true
        self
      end

      def configured?
        @_state.configure
      end

      def started?
        @_state.start
      end

      def stopped?
        @_state.stop
      end

      def before_shutdown?
        @_state.before_shutdown
      end

      def shutdown?
        @_state.shutdown
      end

      def after_shutdown?
        @_state.after_shutdown
      end

      def closed?
        @_state.close
      end

      def terminated?
        @_state.terminate
      end
    end
  end
end
