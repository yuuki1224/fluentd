require_relative '../helper'
require 'fluent/plugin/output'
require 'fluent/plugin/buffer'

require 'json'
require 'time'
require 'timeout'
require 'timecop'

module FluentPluginOutputAsBufferedRetryTest
  class DummyBareOutput < Fluent::Plugin::Output
    def register(name, &block)
      instance_variable_set("@#{name}", block)
    end
  end
  class DummyFullFeatureOutput < DummyBareOutput
    def prefer_buffered_processing
      @prefer_buffered_processing ? @prefer_buffered_processing.call : false
    end
    def prefer_delayed_commit
      @prefer_delayed_commit ? @prefer_delayed_commit.call : false
    end
    def process(tag, es)
      @process ? @process.call(tag, es) : nil
    end
    def format(tag, time, record)
      @format ? @format.call(tag, time, record) : [tag, time, record].to_json
    end
    def write(chunk)
      @write ? @write.call(chunk) : nil
    end
    def try_write(chunk)
      @try_write ? @try_write.call(chunk) : nil
    end
  end
end

class BufferedOutputRetryTest < Test::Unit::TestCase
  def create_output(type=:full)
    case type
    when :bare     then FluentPluginOutputAsBufferedRetryTest::DummyBareOutput.new
    when :full     then FluentPluginOutputAsBufferedRetryTest::DummyFullFeatureOutput.new
    else
      raise ArgumentError, "unknown type: #{type}"
    end
  end
  def create_metadata(timekey: nil, tag: nil, variables: nil)
    Fluent::Plugin::Buffer::Metadata.new(timekey, tag, variables)
  end
  def waiting(seconds)
    begin
      Timeout.timeout(seconds) do
        yield
      end
    rescue Timeout::Error
      STDERR.print *(@i.log.out.logs)
      raise
    end
  end
  def dummy_event_stream
    [
      [ event_time('2016-04-13 18:33:00'), {"name" => "moris", "age" => 36, "message" => "data1"} ],
      [ event_time('2016-04-13 18:33:13'), {"name" => "moris", "age" => 36, "message" => "data2"} ],
      [ event_time('2016-04-13 18:33:32'), {"name" => "moris", "age" => 36, "message" => "data3"} ],
    ]
  end

  teardown do
    if @i
      @i.stop unless @i.stopped?
      @i.before_shutdown unless @i.before_shutdown?
      @i.shutdown unless @i.shutdown?
      @i.after_shutdown unless @i.after_shutdown?
      @i.close unless @i.closed?
      @i.terminate unless @i.terminated?
    end
    Timecop.return
  end

  sub_test_case 'buffered output for retries with exponential backoff' do
    # config_param :retry_forever, :bool, default: false, desc: 'If true, plugin will ignore retry_timeout and retry_max_times options and retry flushing forever.'
    # config_param :retry_timeout, :time, default: 72 * 60 * 60, desc: 'The maximum seconds to retry to flush while failing, until plugin discards buffer chunks.'
    # config_param :retry_max_times, :integer, default: nil, desc: 'The maximum number of times to retry to flush while failing.'
    # config_param :retry_secondary_threshold, :integer, default: 80, desc: 'Percentage of retry_timeout to switch to use secondary while failing.'
    # config_param :retry_type, :enum, list: [:expbackoff, :periodic], default: :expbackoff
    # config_param :retry_wait, :time, default: 1, desc: 'Seconds to wait before next retry to flush, or constant factor of exponential backoff.'
    # config_param :retry_backoff_base, :float, default: 2, desc: 'The base number of exponencial backoff for retries.'
    # config_param :retry_max_interval, :time, default: nil, desc: 'The maximum interval seconds for exponencial backoff between retries while failing.'
    # config_param :retry_randomize, :bool, default: true, desc: 'If true, output plugin will retry after randomized interval not to do burst retries.'

    test 'exponential backoff is default strategy for retries' do
      chunk_key = 'tag'
      hash = {
        'flush_interval' => 1,
        'flush_burst_interval' => 0.1,
        'retry_randomize' => false,
      }
      @i = create_output()
      @i.configure(config_element('ROOT','',{},[config_element('buffer',chunk_key,hash)]))
      @i.register(:prefer_buffered_processing){ true }
      @i.start

      assert_equal :expbackoff, @i.buffer_config.retry_type
      assert_equal 1, @i.buffer_config.retry_wait
      assert_equal 2.0, @i.buffer_config.retry_backoff_base
      assert !@i.buffer_config.retry_randomize

      now = Time.parse('2016-04-13 18:17:00 -0700')
      Timecop.freeze( now )

      retry_state = @i.retry_state( @i.buffer_config.retry_randomize )
      retry_state.step
      assert_equal 1, (retry_state.next_time - now)
      retry_state.step
      assert_equal (1 * (2 ** 1)), (retry_state.next_time - now)
      retry_state.step
      assert_equal (1 * (2 ** 2)), (retry_state.next_time - now)
      retry_state.step
      assert_equal (1 * (2 ** 3)), (retry_state.next_time - now)
    end

    test 'does retries correctly when #write fails' do
      chunk_key = 'tag'
      hash = {
        'flush_interval' => 1,
        'flush_burst_interval' => 0.1,
        'retry_randomize' => false,
        'retry_max_interval' => 60 * 60,
      }
      @i = create_output()
      @i.configure(config_element('ROOT','',{},[config_element('buffer',chunk_key,hash)]))
      @i.register(:prefer_buffered_processing){ true }
      @i.register(:format){|tag,time,record| [tag,time.to_i,record].to_json + "\n" }
      @i.register(:write){|chunk| raise "yay, your #write must fail" }
      @i.start

      now = Time.parse('2016-04-13 18:33:30 -0700')
      Timecop.freeze( now )

      @i.emit("test.tag.1", dummy_event_stream())

      now = Time.parse('2016-04-13 18:33:32 -0700')
      Timecop.freeze( now )

      @i.enqueue_thread_wait

      @i.flush_thread_wakeup
      waiting(4){ Thread.pass until @i.write_count > 0 }

      assert_equal 1, @i.write_count
      assert_equal 1, @i.num_errors

      now = @i.next_flush_time
      Timecop.freeze( now )
      @i.flush_thread_wakeup
      waiting(4){ Thread.pass until @i.write_count > 1 }

      assert_equal 2, @i.write_count
      assert_equal 2, @i.num_errors
    end

    test 'max retry interval is limited by retry_max_interval' do
      chunk_key = 'tag'
      hash = {
        'flush_interval' => 1,
        'flush_burst_interval' => 0.1,
        'retry_randomize' => false,
        'retry_max_interval' => 60,
      }
      @i = create_output()
      @i.configure(config_element('ROOT','',{},[config_element('buffer',chunk_key,hash)]))
      @i.register(:prefer_buffered_processing){ true }
      @i.register(:format){|tag,time,record| [tag,time.to_i,record].to_json + "\n" }
      @i.register(:write){|chunk| raise "yay, your #write must fail" }
      @i.start

      now = Time.parse('2016-04-13 18:33:30 -0700')
      Timecop.freeze( now )

      @i.emit("test.tag.1", dummy_event_stream())

      now = Time.parse('2016-04-13 18:33:32 -0700')
      Timecop.freeze( now )

      @i.enqueue_thread_wait

      @i.flush_thread_wakeup
      waiting(4){ Thread.pass until @i.write_count > 0 }

      assert_equal 1, @i.write_count
      assert_equal 1, @i.num_errors

      prev_write_count = @i.write_count
      prev_num_errors = @i.num_errors

      10.times do
        now = @i.next_flush_time
        Timecop.freeze( now )
        @i.flush_thread_wakeup
        waiting(4){ Thread.pass until @i.write_count > prev_write_count }

        assert_equal (prev_write_count + 1), @i.write_count
        assert_equal (prev_num_errors + 1), @i.num_errors

        prev_write_count = @i.write_count
        prev_num_errors = @i.num_errors
      end
      # expbackoff interval: 1 * 2 ** 10 == 1024
      # but it should be limited by retry_max_interval=60
      assert_equal 60, (@i.next_flush_time - now)
    end

    test 'output plugin give retries up by retry_timeout, and clear queue in buffer' do
      chunk_key = 'tag'
      hash = {
        'flush_interval' => 1,
        'flush_burst_interval' => 0.1,
        'retry_randomize' => false,
        'retry_max_interval' => 60,
      }
      @i = create_output()
      @i.configure(config_element('ROOT','',{},[config_element('buffer',chunk_key,hash)]))
      @i.register(:prefer_buffered_processing){ true }
      @i.register(:format){|tag,time,record| [tag,time.to_i,record].to_json + "\n" }
      @i.register(:write){|chunk| raise "yay, your #write must fail" }
      @i.start

      now = Time.parse('2016-04-13 18:33:30 -0700')
      Timecop.freeze( now )

      @i.emit("test.tag.1", dummy_event_stream())

      now = Time.parse('2016-04-13 18:33:32 -0700')
      Timecop.freeze( now )

      @i.enqueue_thread_wait

      @i.flush_thread_wakeup
      waiting(4){ Thread.pass until @i.write_count > 0 }

      assert_equal 1, @i.write_count
      assert_equal 1, @i.num_errors

      prev_write_count = @i.write_count
      prev_num_errors = @i.num_errors

      10.times do
        now = @i.next_flush_time
        Timecop.freeze( now )
        @i.flush_thread_wakeup
        waiting(4){ Thread.pass until @i.write_count > prev_write_count }

        assert_equal (prev_write_count + 1), @i.write_count
        assert_equal (prev_num_errors + 1), @i.num_errors

        prev_write_count = @i.write_count
        prev_num_errors = @i.num_errors
      end
      # expbackoff interval: 1 * 2 ** 10 == 1024
      # but it should be limited by retry_max_interval=60
      assert_equal 60, (@i.next_flush_time - now)
    end

    test 'output plugin give retries up by retry_max_times, and clear queue in buffer'
  end

  sub_test_case 'bufferd output for retries with periodical retry' do
    test 'periodical retries should retry to write in failing status per retry_wait'
    test 'output plugin give retries up by retry_timeout, and clear queue in buffer'
    test 'retry_max_times can limit maximum times for retries'
  end

  sub_test_case 'buffered output configured as retry_forever' do
    test 'configuration error will be raised if secondary section is configured'
    test 'retry_timeout and retry_max_times will be ignored if retry_forever is true for exponential backoff'
    test 'retry_timeout and retry_max_times will be ignored if retry_forever is true for periodical retries'
  end

  sub_test_case 'buffered output with delayed commit' do
    test 'does retries correctly when #try_write fails'
  end

  sub_test_case 'secondary plugin feature for buffered output with periodical retry' do
    test 'raises configuration error if <buffer> section is specified in <secondary> section'
    test 'warns if secondary plugin is different type from primary one'
    test 'primary plugin will emit event streams to secondary after retries for time of retry_timeout * retry_secondary_threshold'
    test 'exponential backoff interval will be initialized when switched to secondary'
  end

  sub_test_case 'secondary plugin feature for buffered output with exponential backoff' do
    test 'primary plugin will emit event streams to secondary after retries for time of retry_timeout * retry_secondary_threshold'
    test 'retry_wait for secondary is same with one for primary'
  end
end
