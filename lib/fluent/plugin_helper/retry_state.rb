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

module Fluent
  module PluginHelper
    module RetryState
      def retry_state_create(
          title, retry_type, wait, timeout,
          forever: false, max_steps: nil, backoff_base: 2, max_interval: nil, randomize: true, randomize_width: 0.125,
          secondary: false, secondary_threshold: 0.8
      )
        case retry_type
        when :expbackoff
          ExponentialBackOffRetry.new(title, wait, timeout, forever, max_steps, randomize, randomize_width, backoff_base, max_interval, secondary, secondary_threshold)
        when :periodic
          PeriodicRetry.new(title, wait, timeout, forever, max_steps, randomize, randomize_width, secondary, secondary_threshold)
        else
          raise "BUG: unknown retry_type specified: '#{retry_type}'"
        end
      end

      class RetryStateMachine
        attr_reader :title, :start, :steps, :next_time, :timeout_at, :current, :secondary_transition_at, :secondary_transition_steps

        def initialize(title, wait, timeout, forever, max_steps, randomize, randomize_width, secondary, secondary_threshold)
          @title = title

          @start = current_time
          @steps = 0
          @next_time = nil # should be initialized for first retry by child class

          @timeout = timeout
          @timeout_at = @start + timeout
          @current = :primary

          if randomize_width < 0 || randomize_width > 0.5
            raise "BUG: randomize_width MUST be between 0 and 0.5"
          end

          @randomize = randomize
          @randomize_width = randomize_width

          if forever && secondary
            raise "BUG: forever and secondary are exclusive to each other"
          end

          @forever = forever
          @max_steps = max_steps

          @secondary = secondary
          @secondary_threshold = secondary_threshold
          if @secondary
            raise "BUG: secondary_transition_threshold MUST be between 0 and 1" if @secondary_threshold <= 0 || @secondary_threshold >= 1
            @secondary_transition_at = @start + timeout * @secondary_threshold
            @secondary_transition_steps = nil
          end
        end

        def current_time
          Time.now
        end

        def randomize(interval)
          return interval unless @randomize

          interval + (interval * @randomize_width * (2 * rand - 1.0))
        end

        def calc_next_time
          if @forever || !@secondary # primary
            naive = naive_next_time(@steps)
            if @forever
              naive
            elsif naive >= @timeout_at
              @timeout_at
            else
              naive
            end
          elsif @current == :primary && @secondary
            naive = naive_next_time(@steps)
            if naive >= @secondary_transition_at
              @secondary_transition_at
            else
              naive
            end
          elsif @current == :secondary
            naive = naive_next_time(@steps - @secondary_transition_steps + 1)
            if naive >= @timeout_at
              @timeout_at
            else
              naive
            end
          else
            raise "BUG: it's out of design"
          end
        end

        def naive_next_time(retry_times)
          raise NotImplementedError
        end

        def secondary?
          @secondary && (@current == :secondary || current_time >= @secondary_transition_at)
        end

        def step
          @steps += 1
          if @secondary && @current != :secondary && current_time >= @secondary_transition_at
            @current = :secondary
            @secondary_transition_steps = @steps
          end
          @next_time = calc_next_time
          nil
        end

        def limit?
          if @forever
            false
          else
            @next_time >= @timeout_at || !!(@max_steps && @steps >= @max_steps)
          end
        end
      end

      class ExponentialBackOffRetry < RetryStateMachine
        def initialize(title, wait, timeout, forever, max_steps, randomize, randomize_width, backoff_base, max_interval, secondary, secondary_threathold)
          super(title, wait, timeout, forever, max_steps, randomize, randomize_width, secondary, secondary_threathold)
          @constant_factor = wait
          @backoff_base = backoff_base
          @max_interval = max_interval

          @next_time = @start + @constant_factor
        end

        def naive_next_time(retry_next_times)
          interval = @constant_factor * ( @backoff_base ** ( retry_next_times - 1 ) )
          intr = if @max_interval && interval > @max_interval
                   @max_interval
                 else
                   interval
                 end
          current_time + randomize(intr)
        end
      end

      class PeriodicRetry < RetryStateMachine
        def initialize(title, wait, timeout, forever, max_steps, randomize, randomize_width, secondary, secondary_threathold)
          super(title, wait, timeout, forever, max_steps, randomize, randomize_width, secondary, secondary_threathold)
          @retry_wait = wait
          @next_time = @start + @retry_wait
        end

        def naive_next_time(retry_next_times)
          current_time + randomize(@retry_wait)
        end
      end
    end
  end
end
