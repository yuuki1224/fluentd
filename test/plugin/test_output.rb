require_relative '../helper'
require 'fluent/plugin/output'
require 'fluent/plugin/buffer'

def create_output(type=:full)
  case type
  when :bare     then FluentPluginOutputTest::DummyBareOutput.new
  when :sync     then FluentPluginOutputTest::DummySyncOutput.new
  when :buffered then FluentPluginOutputTest::DummyAsyncOutput.new
  when :delayed  then FluentPluginOutputTest::DummyDelayedOutput.new
  when :full     then FluentPluginOutputTest::DummyFullFeatureOutput.new
  else
    raise ArgumentError, "unknown type: #{type}"
  end
end
def create_metadata(timekey: nil, tag: nil, variables: nil)
  Fluent::Plugin::Buffer::Metadata.new(timekey, tag, variables)
end

module FluentPluginOutputTest
  class DummyBareOutput < Fluent::Plugin::Output
  end
  class DummySyncOutput < Fluent::Plugin::Output
    def process(tag, es)
    end
  end
  class DummyAsyncOutput < Fluent::Plugin::Output
    attr_accessor :formatter
    def format(tag, time, record)
      @formatter ? @formatter.call(tag, time, record) : ''
    end
    def write(chunk)
    end
  end
  class DummyDelayedOutput < Fluent::Plugin::Output
    attr_accessor :formatter
    def format(tag, time, record)
      @formatter ? @formatter.call(tag, time, record) : ''
    end
    def try_write(chunk)
    end
  end
  class DummyFullFeatureOutput < Fluent::Plugin::Output
    attr_accessor :prefer_buffered, :prefer_delayed, :formatter
    def initialize
      super
      @formatter = nil
      @prefer_buffered = @prefer_delayed = false
    end
    def prefer_buffered_processing
      @prefer_buffered
    end
    def prefer_delayed_commit
      @prefer_delayed
    end
    def process(tag, es)
    end
    def format(tag, time, record)
      @formatter ? @formatter.call(tag, time, record) : ''
    end
    def write(chunk)
    end
    def try_write(chunk)
    end
  end
end

class OutputTest < Test::Unit::TestCase
  sub_test_case 'basic output feature' do
    setup do
      @i = FluentPluginOutputTest::DummyFullFeatureOutput.new
    end

    test '#implement? can return features for plugin instances' do
      i1 = FluentPluginOutputTest::DummyBareOutput.new
      assert !i1.implement?(:synchronous)
      assert !i1.implement?(:buffered)
      assert !i1.implement?(:delayed_commit)

      i2 = FluentPluginOutputTest::DummySyncOutput.new
      assert i2.implement?(:synchronous)
      assert !i2.implement?(:buffered)
      assert !i2.implement?(:delayed_commit)

      i3 = FluentPluginOutputTest::DummyAsyncOutput.new
      assert !i3.implement?(:synchronous)
      assert i3.implement?(:buffered)
      assert !i3.implement?(:delayed_commit)

      i4 = FluentPluginOutputTest::DummyDelayedOutput.new
      assert !i4.implement?(:synchronous)
      assert !i4.implement?(:buffered)
      assert i4.implement?(:delayed_commit)

      i5 = FluentPluginOutputTest::DummyFullFeatureOutput.new
      assert i5.implement?(:synchronous)
      assert i5.implement?(:buffered)
      assert i5.implement?(:delayed_commit)
    end

    test 'plugin lifecycle for configure/start/stop/before_shutdown/shutdown/after_shutdown/close/terminate' do
      assert !@i.configured?
      @i.configure(config_element())
      assert @i.configured?
      assert !@i.started?
      @i.start
      assert @i.started?
      assert !@i.stopped?
      @i.stop
      assert @i.stopped?
      assert !@i.before_shutdown?
      @i.before_shutdown
      assert @i.before_shutdown?
      assert !@i.shutdown?
      @i.shutdown
      assert @i.shutdown?
      assert !@i.after_shutdown?
      @i.after_shutdown
      assert @i.after_shutdown?
      assert !@i.closed?
      @i.close
      assert @i.closed?
      assert !@i.terminated?
      @i.terminate
      assert @i.terminated?
    end

    test '#extract_placeholders does nothing if chunk key is not specified' do
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', '')]))
      assert !@i.chunk_key_time
      assert !@i.chunk_key_tag
      assert_equal [], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail"
      t = Time.parse('2016-04-11 20:30:00 +0900').to_i
      v = {key1: "value1", key2: "value2"}
      m = create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
      assert_equal tmpl, @i.extract_placeholders(tmpl, m)
    end

    test '#extract_placeholders can extract time if time key and range are configured' do
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'time', {'timekey_range' => 60*30, 'timekey_zone' => "+0900"})]))
      assert @i.chunk_key_time
      assert !@i.chunk_key_tag
      assert_equal [], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail"
      t = Time.parse('2016-04-11 20:30:00 +0900').to_i
      v = {key1: "value1", key2: "value2"}
      m = create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
      assert_equal "/mypath/2016/04/11/20-30/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail", @i.extract_placeholders(tmpl, m)
    end

    test '#extract_placeholders can extract tag and parts of tag if tag is configured' do
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'tag', {})]))
      assert !@i.chunk_key_time
      assert @i.chunk_key_tag
      assert_equal [], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail"
      t = Time.parse('2016-04-11 20:30:00 +0900').to_i
      v = {key1: "value1", key2: "value2"}
      m = create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
      assert_equal "/mypath/%Y/%m/%d/%H-%M/fluentd.test.output/test/output/${key1}/${key2}/tail", @i.extract_placeholders(tmpl, m)
    end

    test '#extract_placeholders can extract variables if variables are configured' do
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'key1,key2', {})]))
      assert !@i.chunk_key_time
      assert !@i.chunk_key_tag
      assert_equal ['key1','key2'], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail"
      t = Time.parse('2016-04-11 20:30:00 +0900').to_i
      v = {key1: "value1", key2: "value2"}
      m = create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
      assert_equal "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[1]}/${tag[2]}/value1/value2/tail", @i.extract_placeholders(tmpl, m)
    end

    test '#extract_placeholders can extract all chunk keys if configured' do
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'time,tag,key1,key2', {'timekey_range' => 60*30, 'timekey_zone' => "+0900"})]))
      assert @i.chunk_key_time
      assert @i.chunk_key_tag
      assert_equal ['key1','key2'], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[1]}/${tag[2]}/${key1}/${key2}/tail"
      t = Time.parse('2016-04-11 20:30:00 +0900').to_i
      v = {key1: "value1", key2: "value2"}
      m = create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
      assert_equal "/mypath/2016/04/11/20-30/fluentd.test.output/test/output/value1/value2/tail", @i.extract_placeholders(tmpl, m)
    end

    test '#extract_placeholders removes out-of-range tag part and unknown variable placeholders' do
      @i.configure(config_element('ROOT', '', {}, [config_element('buffer', 'time,tag,key1,key2', {'timekey_range' => 60*30, 'timekey_zone' => "+0900"})]))
      assert @i.chunk_key_time
      assert @i.chunk_key_tag
      assert_equal ['key1','key2'], @i.chunk_keys
      tmpl = "/mypath/%Y/%m/%d/%H-%M/${tag}/${tag[3]}/${tag[4]}/${key3}/${key4}/tail"
      t = Time.parse('2016-04-11 20:30:00 +0900').to_i
      v = {key1: "value1", key2: "value2"}
      m = create_metadata(timekey: t, tag: 'fluentd.test.output', variables: v)
      assert_equal "/mypath/2016/04/11/20-30/fluentd.test.output/////tail", @i.extract_placeholders(tmpl, m)
    end

    test '#metadata returns object which contains tag/timekey/variables from records as specified in configuration' do
      tag = 'test.output'
      time = Time.parse('2016-04-12 15:31:23 -0700').to_i
      timekey = Time.parse('2016-04-12 15:00:00 -0700').to_i
      record = {"key1" => "value1", "num1" => 1, "message" => "my message"}

      i1 = create_output(:buffered)
      i1.configure(config_element('ROOT','',{},[config_element('buffer', '')]))
      assert_equal create_metadata(), i1.metadata(tag, time, record)

      i2 = create_output(:buffered)
      i2.configure(config_element('ROOT','',{},[config_element('buffer', 'tag')]))
      assert_equal create_metadata(tag: tag), i2.metadata(tag, time, record)

      i3 = create_output(:buffered)
      i3.configure(config_element('ROOT','',{},[config_element('buffer', 'time', {"timekey_range" => 3600, "timekey_zone" => "-0700"})]))
      assert_equal create_metadata(timekey: timekey), i3.metadata(tag, time, record)

      i4 = create_output(:buffered)
      i4.configure(config_element('ROOT','',{},[config_element('buffer', 'key1', {})]))
      assert_equal create_metadata(variables: {key1: "value1"}), i4.metadata(tag, time, record)

      i5 = create_output(:buffered)
      i5.configure(config_element('ROOT','',{},[config_element('buffer', 'key1,num1', {})]))
      assert_equal create_metadata(variables: {key1: "value1", num1: 1}), i5.metadata(tag, time, record)

      i6 = create_output(:buffered)
      i6.configure(config_element('ROOT','',{},[config_element('buffer', 'tag,time', {"timekey_range" => 3600, "timekey_zone" => "-0700"})]))
      assert_equal create_metadata(timekey: timekey, tag: tag), i6.metadata(tag, time, record)

      i7 = create_output(:buffered)
      i7.configure(config_element('ROOT','',{},[config_element('buffer', 'tag,num1', {"timekey_range" => 3600, "timekey_zone" => "-0700"})]))
      assert_equal create_metadata(tag: tag, variables: {num1: 1}), i7.metadata(tag, time, record)

      i8 = create_output(:buffered)
      i8.configure(config_element('ROOT','',{},[config_element('buffer', 'time,tag,key1', {"timekey_range" => 3600, "timekey_zone" => "-0700"})]))
      assert_equal create_metadata(timekey: timekey, tag: tag, variables: {key1: "value1"}), i8.metadata(tag, time, record)
    end

    test '#emit calls #process via #emit_sync for non-buffered output' do
      i = create_output(:sync)
      process_called = false
      (class << i; self; end).module_eval do
        define_method(:process){ |tag, es|
          process_called = true
        }
      end
      i.configure(config_element())
      i.start

      t = Time.now.to_i
      i.emit('tag', [ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ])

      assert process_called

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end

    test '#emit calls #format for buffered output' do
      i = create_output(:buffered)
      format_called_times = 0
      (class << i; self; end).module_eval do
        define_method(:format){ |tag, time, record|
          format_called_times += 1
          ''
        }
      end
      i.configure(config_element())
      i.start

      t = Time.now.to_i
      i.emit('tag', [ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ])

      assert_equal 2, format_called_times

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end

    test '#prefer_buffered_processing (returns false) decides non-buffered without <buffer> section' do
      i = create_output(:full)

      process_called = false
      (class << i; self; end).module_eval do
        define_method(:process){ |tag, es|
          process_called = true
        }
      end
      format_called_times = 0
      (class << i; self; end).module_eval do
        define_method(:format){ |tag, time, record|
          format_called_times += 1
          ''
        }
      end

      i.configure(config_element())
      i.prefer_buffered = false # delayed decision is possible to change after (output's) configure
      i.start

      assert !i.prefer_buffered
      assert !i.prefer_buffered_processing

      t = Time.now.to_i
      i.emit('tag', [ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ])

      assert process_called
      assert_equal 0, format_called_times

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end

    test '#prefer_buffered_processing (returns true) decides buffered without <buffer> section' do
      i = create_output(:full)

      process_called = false
      (class << i; self; end).module_eval do
        define_method(:process){ |tag, es|
          process_called = true
        }
      end
      format_called_times = 0
      (class << i; self; end).module_eval do
        define_method(:format){ |tag, time, record|
          format_called_times += 1
          ''
        }
      end

      i.configure(config_element())
      i.prefer_buffered = true # delayed decision is possible to change after (output's) configure
      i.start

      assert i.prefer_buffered
      assert i.prefer_buffered_processing

      t = Time.now.to_i
      i.emit('tag', [ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ])

      assert !process_called
      assert_equal 2, format_called_times

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end

    test 'output plugin will call #write for normal buffered plugin to flush buffer chunks' do
      i = create_output(:buffered)
      i.formatter = ->(tag, time, record){ [tag,time,record].to_json }
      write_called = false
      (class << i; self; end).module_eval do
        define_method(:write){ |chunk| write_called = true }
      end

      i.configure(config_element('ROOT', '', {}, [config_element('buffer', '', {"flush_mode" => "immediate"})]))
      i.start

      t = Time.now.to_i
      i.emit('tag', [ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ])
      i.force_flush

      assert write_called

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end

    test 'output plugin will call #try_write for plugin supports delayed commit only to flush buffer chunks' do
      i = create_output(:delayed)
      i.formatter = ->(tag, time, record){ [tag,time,record].to_json }
      try_write_called = false
      (class << i; self; end).module_eval do
        define_method(:try_write){ |chunk| try_write_called = true; commit_write(chunk.unique_id) }
      end

      i.configure(config_element('ROOT', '', {}, [config_element('buffer', '', {"flush_mode" => "immediate"})]))
      i.start

      t = Time.now.to_i
      i.emit('tag', [ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ])
      i.force_flush

      assert try_write_called

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end

    test '#prefer_delayed_commit (returns false) decides delayed commit is disabled if both are implemented' do
      i = create_output(:full)
      i.formatter = ->(tag, time, record){ [tag,time,record].to_json }
      write_called = false
      try_write_called = false
      (class << i; self; end).module_eval do
        define_method(:write){ |chunk| write_called = true }
      end
      (class << i; self; end).module_eval do
        define_method(:try_write){ |chunk| try_write_called = true; commit_write(chunk.unique_id) }
      end

      i.configure(config_element('ROOT', '', {}, [config_element('buffer', '', {"flush_mode" => "immediate"})]))
      i.prefer_delayed = false # delayed decision is possible to change after (output's) configure
      i.start

      assert !i.prefer_delayed
      assert !i.prefer_delayed_commit

      t = Time.now.to_i
      i.emit('tag', [ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ])
      i.force_flush

      assert write_called
      assert !try_write_called

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end

    test '#prefer_delayed_commit (returns true) decides delayed commit is enabled if both are implemented' do
      i = create_output(:full)
      i.formatter = ->(tag, time, record){ [tag,time,record].to_json }
      write_called = false
      try_write_called = false
      (class << i; self; end).module_eval do
        define_method(:write){ |chunk| write_called = true }
      end
      (class << i; self; end).module_eval do
        define_method(:try_write){ |chunk| try_write_called = true; commit_write(chunk.unique_id) }
      end

      i.configure(config_element('ROOT', '', {}, [config_element('buffer', '', {"flush_mode" => "immediate"})]))
      i.prefer_delayed = true # delayed decision is possible to change after (output's) configure
      i.start

      assert i.prefer_delayed
      assert i.prefer_delayed_commit

      t = Time.now.to_i
      i.emit('tag', [ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ])
      i.force_flush

      assert !write_called
      assert try_write_called

      i.stop; i.before_shutdown; i.shutdown; i.after_shutdown; i.close; i.terminate
    end
  end

  sub_test_case 'sync output feature' do
    test 'raises configuration error if <buffer> section is specified'
    test 'raises configuration error if <secondary> section is specified'
    test '#process is called for each event streams'
  end

  sub_test_case 'buffered output feature without any buffer key, flush_mode: none' do
    test '#start does not create enqueue thread, but creates flush threads'
    test '#format is called for each event streams'
    test '#write is called only when chunk bytes limit exceeded, and buffer chunk is purged'
    test 'flush_at_shutdown work well when plugin is shutdown'
  end

  sub_test_case 'buffered output feature without any buffer key, flush_mode: fast' do
    test '#start creates enqueue thread and flush threads'
    test '#format is called for each event streams'
    test '#write is called when chunk bytes limit exceeded, or per flush_interval, and buffer chunk is purged'
    test 'flush_at_shutdown work well when plugin is shutdown'
  end

  sub_test_case 'buffered output feature without any buffer key, flush_mode: immediate' do
    test '#start does not create enqueue thread, but creates flush threads'
    test '#format is called for each event streams'
    test '#write is called every time for each emits, and buffer chunk is purged'
    test 'flush_at_shutdown work well when plugin is shutdown'
  end

  sub_test_case 'buffered output feature with timekey and range' do
    test '#configure raises config error if timekey_range is not specified'
    test 'default flush mode is set to :none'
    test '#start creates enqueue thread and flush threads'
    test '#format is called for each event streams'
    test '#write is called per time ranges after timekey_wait, and buffer chunk is purged'
    test 'flush_at_shutdown work well when plugin is shutdown'
  end

  sub_test_case 'buffered output feature with tag key' do
    test 'default flush mode is set to :fast'
    test '#start creates enqueue thread and flush threads'
    test '#format is called for each event streams'
    test '#write is called per tags, per flush_interval & chunk sizes, and buffer chunk is purged'
    test 'flush_at_shutdown work well when plugin is shutdown'
  end

  sub_test_case 'buffered output feature with variables' do
    test 'default flush mode is set to :fast'
    test '#start creates enqueue thread and flush threads'
    test '#format is called for each event streams'
    test '#write is called per value combination of variables, per flush_interval & chunk sizes, and buffer chunk is purged'
    test 'flush_at_shutdown work well when plugin is shutdown'
  end

  sub_test_case 'buffered output feature with many keys' do
    test 'default flush mode is set to :fast if keys does not include time'
    test 'default flush mode is set to :none if keys includes time'
  end

  sub_test_case 'buffered output feature with delayed commit' do
    test '#format is called for each event streams'
    test '#try_write is called per flush, buffer chunk is not purged'
    test 'buffer chunk is purged when plugin calls #commit_write'
    test '#try_rollback_write can rollback buffer chunks for delayed commit after timeout'
    test '#try_rollback_all will be called for all waiting chunks when shutdown'
  end

  sub_test_case 'buffered output for retries with exponential backoff' do
    test 'exponential backoff is default strategy for retries'
    test 'max retry interval is limited by retry_max_interval'
    test 'retry_timeout can limit total time for retries'
    test 'retry_max_times can limit maximum times for retries'
  end

  sub_test_case 'bufferd output for retries with periodical retry' do
    test 'periodical retries should retry to write in failing status per retry_wait'
    test 'retry_timeout can limit total time for retries'
    test 'retry_max_times can limit maximum times for retries'
  end

  sub_test_case 'buffered output configured as retry_forever' do
    test 'configuration error will be raised if secondary section is configured'
    test 'retry_timeout and retry_max_times will be ignored if retry_forever is true for exponential backoff'
    test 'retry_timeout and retry_max_times will be ignored if retry_forever is true for periodical retries'
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
