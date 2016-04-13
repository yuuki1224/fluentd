require_relative '../helper'
require 'fluent/plugin/output'
require 'fluent/plugin/buffer'

require 'json'
require 'time'
require 'timeout'
require 'timecop'

module FluentPluginOutputAsBufferedTest
  class DummyBareOutput < Fluent::Plugin::Output
    def register(name, &block)
      instance_variable_set("@#{name}", block)
    end
  end
  class DummySyncOutput < DummyBareOutput
    def process(tag, es)
      @process ? @process.call(tag, es) : nil
    end
  end
  class DummyAsyncOutput < DummyBareOutput
    def format(tag, time, record)
      @format ? @format.call(tag, time, record) : [tag, time, record].to_json
    end
    def write(chunk)
      @write ? @write.call(chunk) : nil
    end
  end
  class DummyDelayedOutput < DummyBareOutput
    def format(tag, time, record)
      @format ? @format.call(tag, time, record) : [tag, time, record].to_json
    end
    def try_write(chunk)
      @try_write ? @try_write.call(chunk) : nil
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

class OutputTest < Test::Unit::TestCase
  def create_output(type=:full)
    case type
    when :bare     then FluentPluginOutputAsBufferedTest::DummyBareOutput.new
    when :sync     then FluentPluginOutputAsBufferedTest::DummySyncOutput.new
    when :buffered then FluentPluginOutputAsBufferedTest::DummyAsyncOutput.new
    when :delayed  then FluentPluginOutputAsBufferedTest::DummyDelayedOutput.new
    when :full     then FluentPluginOutputAsBufferedTest::DummyFullFeatureOutput.new
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
      p @i.log.out.logs
      raise
    end
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

  sub_test_case 'buffered output feature without any buffer key, flush_mode: none' do
    setup do
      hash = {
        'flush_mode' => 'none',
        'flush_threads' => 2,
        'chunk_bytes_limit' => 1024,
      }
      @i = create_output(:buffered)
      @i.configure(config_element('ROOT','',{},[config_element('buffer','',hash)]))
      @i.start
    end

    test '#start does not create enqueue thread, but creates flush threads' do
      @i.thread_wait_until_start

      assert @i.thread_exist?(:flush_thread_0)
      assert @i.thread_exist?(:flush_thread_1)
      assert !@i.thread_exist?(:enqueue_thread)
    end

    test '#format is called for each events' do
      ary = []
      @i.register(:format){|tag, time, record| ary << [tag, time, record]; '' }

      t = event_time()
      es = [ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ]

      5.times do
        @i.emit('tag.test', es)
      end

      assert_equal 10, ary.size
      5.times do |i|
        assert_equal ["tag.test", t, {"key" => "value1"}], ary[i*2]
        assert_equal ["tag.test", t, {"key" => "value2"}], ary[i*2+1]
      end
    end

    test '#write is called only when chunk bytes limit exceeded, and buffer chunk is purged' do
      ary = []
      @i.register(:write){|chunk| ary << chunk.read }

      tag = "test.tag"
      t = event_time()
      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      event_size = [tag, t, r].to_json.size # 195

      (1024 / event_size).times do |i|
        @i.emit("test.tag", [ [t, r] ])
      end
      assert{ @i.buffer.queue.size == 0 && ary.size == 0 }

      staged_chunk = @i.buffer.stage[@i.buffer.stage.keys.first]
      assert{ staged_chunk.records != 0 }

      @i.emit("test.tag", [ [t, r] ])

      assert{ @i.buffer.queue.size > 0 || @i.buffer.dequeued.size > 0 || ary.size > 0 }

      waiting(10) do
        Thread.pass until @i.buffer.queue.size == 0 && @i.buffer.dequeued.size == 0
        Thread.pass until staged_chunk.records == 0
      end

      assert_equal 1, ary.size
      assert_equal [tag,t,r].to_json * (1024 / event_size), ary.first
    end

    test 'flush_at_shutdown work well when plugin is shutdown' do
      ary = []
      @i.register(:write){|chunk| ary << chunk.read }

      tag = "test.tag"
      t = event_time()
      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      event_size = [tag, t, r].to_json.size # 195

      (1024 / event_size).times do |i|
        @i.emit("test.tag", [ [t, r] ])
      end
      assert{ @i.buffer.queue.size == 0 && ary.size == 0 }

      @i.stop
      @i.before_shutdown
      @i.shutdown
      @i.after_shutdown

      waiting(10) do
        Thread.pass until ary.size == 1
      end
      assert_equal [tag,t,r].to_json * (1024 / event_size), ary.first
    end
  end

  sub_test_case 'buffered output feature without any buffer key, flush_mode: fast' do
    setup do
      hash = {
        'flush_mode' => 'fast',
        'flush_interval' => 1,
        'flush_threads' => 1,
        'chunk_bytes_limit' => 1024,
      }
      @i = create_output(:buffered)
      @i.configure(config_element('ROOT','',{},[config_element('buffer','',hash)]))
      @i.start
    end

    test '#start creates enqueue thread and flush threads' do
      @i.thread_wait_until_start

      assert @i.thread_exist?(:flush_thread_0)
      assert @i.thread_exist?(:enqueue_thread)
    end

    test '#format is called for each event streams' do
      ary = []
      @i.register(:format){|tag, time, record| ary << [tag, time, record]; '' }

      t = event_time()
      es = [ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ]

      5.times do
        @i.emit('tag.test', es)
      end

      assert_equal 10, ary.size
      5.times do |i|
        assert_equal ["tag.test", t, {"key" => "value1"}], ary[i*2]
        assert_equal ["tag.test", t, {"key" => "value2"}], ary[i*2+1]
      end
    end

    test '#write is called per flush_interval, and buffer chunk is purged' do
      @i.thread_wait_until_start

      ary = []
      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:write){|chunk| chunk.read.split("\n").reject{|l| l.empty? }.each{|data| ary << data } }

      tag = "test.tag"
      t = event_time()
      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      event_size = [tag, t, r].to_json.size # 195

      3.times do |i|
        rand_records = rand(1..5)
        es = [ [t, r] ] * rand_records
        assert_equal rand_records, es.size

        @i.interrupt_flushes

        @i.emit("test.tag", es)

        assert{ @i.buffer.stage.size == 1 }

        staged_chunk = @i.instance_eval{ @buffer.stage[@buffer.stage.keys.first] }
        assert{ staged_chunk.records != 0 }

        @i.enqueue_thread_wait

        waiting(10) do
          Thread.pass until @i.buffer.queue.size == 0 && @i.buffer.dequeued.size == 0
          Thread.pass until staged_chunk.records == 0
        end

        assert_equal rand_records, ary.size
        ary.reject!{|e| true }
      end
    end

    test 'flush_at_shutdown work well when plugin is shutdown' do
      ary = []
      @i.register(:write){|chunk| ary << chunk.read }

      tag = "test.tag"
      t = event_time()
      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      event_size = [tag, t, r].to_json.size # 195

      (1024 / event_size).times do |i|
        @i.emit("test.tag", [ [t, r] ])
      end
      assert{ @i.buffer.queue.size == 0 && ary.size == 0 }

      @i.stop
      @i.before_shutdown
      @i.shutdown
      @i.after_shutdown

      waiting(10) do
        Thread.pass until ary.size == 1
      end
      assert_equal [tag,t,r].to_json * (1024 / event_size), ary.first
    end
  end

  sub_test_case 'buffered output feature without any buffer key, flush_mode: immediate' do
    setup do
      hash = {
        'flush_mode' => 'immediate',
        'flush_threads' => 1,
        'chunk_bytes_limit' => 1024,
      }
      @i = create_output(:buffered)
      @i.configure(config_element('ROOT','',{},[config_element('buffer','',hash)]))
      @i.start
    end

    test '#start does not create enqueue thread, but creates flush threads' do
      @i.thread_wait_until_start

      assert @i.thread_exist?(:flush_thread_0)
      assert !@i.thread_exist?(:enqueue_thread)
    end

    test '#format is called for each event streams' do
      ary = []
      @i.register(:format){|tag, time, record| ary << [tag, time, record]; '' }

      t = event_time()
      es = [ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ]

      5.times do
        @i.emit('tag.test', es)
      end

      assert_equal 10, ary.size
      5.times do |i|
        assert_equal ["tag.test", t, {"key" => "value1"}], ary[i*2]
        assert_equal ["tag.test", t, {"key" => "value2"}], ary[i*2+1]
      end
    end

    test '#write is called every time for each emits, and buffer chunk is purged' do
      @i.thread_wait_until_start

      ary = []
      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:write){|chunk| chunk.read.split("\n").reject{|l| l.empty? }.each{|data| ary << data } }

      tag = "test.tag"
      t = event_time()
      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      event_size = [tag, t, r].to_json.size # 195

      3.times do |i|
        rand_records = rand(1..5)
        es = [ [t, r] ] * rand_records
        assert_equal rand_records, es.size
        @i.emit("test.tag", es)

        assert{ @i.buffer.stage.size == 0 && (@i.buffer.queue.size == 1 || @i.buffer.dequeued.size == 1 || ary.size > 0) }

        waiting(10) do
          Thread.pass until @i.buffer.queue.size == 0 && @i.buffer.dequeued.size == 0
        end

        assert_equal rand_records, ary.size
        ary.reject!{|e| true }
      end
    end

    test 'flush_at_shutdown work well when plugin is shutdown' do
      ary = []
      @i.register(:write){|chunk| ary << chunk.read }

      tag = "test.tag"
      t = event_time()
      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      @i.emit("test.tag", [ [t, r] ])

      @i.stop
      @i.before_shutdown
      @i.shutdown
      @i.after_shutdown

      waiting(10) do
        Thread.pass until ary.size == 1
      end
      assert_equal [tag,t,r].to_json, ary.first
    end
  end

  sub_test_case 'buffered output feature with timekey and range' do
    setup do
      chunk_key = 'time'
      hash = {
        'timekey_range' => 30, # per 30seconds
        'timekey_wait' => 5, # 5 second delay for flush
        'flush_threads' => 1,
      }
      @i = create_output(:buffered)
      @i.configure(config_element('ROOT','',{},[config_element('buffer',chunk_key,hash)]))
      @i.start
    end

    test '#configure raises config error if timekey_range is not specified' do
      i = create_output(:buffered)
      assert_raise Fluent::ConfigError do
        i.configure(config_element('ROOT','',{},[config_element('buffer','time',)]))
      end
    end

    test 'default flush_mode is set to :none' do
      assert_equal :none, @i.instance_eval{ @flush_mode }
    end

    test '#start creates enqueue thread and flush threads' do
      @i.thread_wait_until_start

      assert @i.thread_exist?(:flush_thread_0)
      assert @i.thread_exist?(:enqueue_thread)
    end

    test '#format is called for each event streams' do
      ary = []
      @i.register(:format){|tag, time, record| ary << [tag, time, record]; '' }

      t = event_time()
      es = [ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ]

      5.times do
        @i.emit('tag.test', es)
      end

      assert_equal 10, ary.size
      5.times do |i|
        assert_equal ["tag.test", t, {"key" => "value1"}], ary[i*2]
        assert_equal ["tag.test", t, {"key" => "value2"}], ary[i*2+1]
      end
    end

    test '#write is called per time ranges after timekey_wait, and buffer chunk is purged' do
      Timecop.freeze( Time.parse('2016-04-13 14:04:00 +0900') )

      @i.thread_wait_until_start

      ary = []
      metachecks = []

      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:write){|chunk| chunk.read.split("\n").reject{|l| l.empty? }.each{|data| e = JSON.parse(data); ary << e; metachecks << (chunk.metadata.timekey.to_i <= e[1].to_i && e[1].to_i < chunk.metadata.timekey.to_i + 30) } }

      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      ts = [
        Fluent::EventTime.parse('2016-04-13 14:03:21 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:23 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:29 +0900'),
        Fluent::EventTime.parse('2016-04-13 14:03:30 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:33 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:38 +0900'),
        Fluent::EventTime.parse('2016-04-13 14:03:43 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:49 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:51 +0900'),
        Fluent::EventTime.parse('2016-04-13 14:04:00 +0900'), Fluent::EventTime.parse('2016-04-13 14:04:01 +0900'),
      ]
      events = [
        ["test.tag.1", ts[0], r], # range 14:03:00 - 03:29
        ["test.tag.2", ts[1], r],
        ["test.tag.1", ts[2], r],
        ["test.tag.1", ts[3], r], # range 14:03:30 - 04:00
        ["test.tag.1", ts[4], r],
        ["test.tag.1", ts[5], r],
        ["test.tag.1", ts[6], r],
        ["test.tag.1", ts[7], r],
        ["test.tag.2", ts[8], r],
        ["test.tag.1", ts[9], r], # range 14:04:00 - 04:29
        ["test.tag.2", ts[10], r],
      ]

      assert_equal 0, @i.write_count

      @i.interrupt_flushes

      events.shuffle.each do |tag, time, record|
        @i.emit(tag, [ [time, record] ])
      end
      assert{ @i.buffer.stage.size == 3 }
      assert{ @i.write_count == 0 }

      @i.enqueue_thread_wait

      waiting(4) do
        Thread.pass until @i.write_count > 0
      end

      assert{ @i.buffer.stage.size == 2 && @i.write_count == 1 }

      assert_equal 3, ary.size
      assert_equal 2, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 1, ary.select{|e| e[0] == "test.tag.2" }.size

      Timecop.freeze( Time.parse('2016-04-13 14:04:04 +0900') )

      @i.enqueue_thread_wait

      assert{ @i.buffer.stage.size == 2 && @i.write_count == 1 }

      Timecop.freeze( Time.parse('2016-04-13 14:04:06 +0900') )

      @i.enqueue_thread_wait
      waiting(4) do
        Thread.pass until @i.write_count > 1
      end

      assert{ @i.buffer.stage.size == 1 && @i.write_count == 2 }

      assert_equal 9, ary.size
      assert_equal 7, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 2, ary.select{|e| e[0] == "test.tag.2" }.size

      assert metachecks.all?{|e| e }
    end

    test 'flush_at_shutdown work well when plugin is shutdown' do
      Timecop.freeze( Time.parse('2016-04-13 14:04:00 +0900') )

      @i.thread_wait_until_start

      ary = []
      metachecks = []

      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:write){|chunk| chunk.read.split("\n").reject{|l| l.empty? }.each{|data| e = JSON.parse(data); ary << e; metachecks << (chunk.metadata.timekey.to_i <= e[1].to_i && e[1].to_i < chunk.metadata.timekey.to_i + 30) } }

      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      ts = [
        Fluent::EventTime.parse('2016-04-13 14:03:21 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:23 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:29 +0900'),
        Fluent::EventTime.parse('2016-04-13 14:03:30 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:33 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:38 +0900'),
        Fluent::EventTime.parse('2016-04-13 14:03:43 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:49 +0900'), Fluent::EventTime.parse('2016-04-13 14:03:51 +0900'),
        Fluent::EventTime.parse('2016-04-13 14:04:00 +0900'), Fluent::EventTime.parse('2016-04-13 14:04:01 +0900'),
      ]
      events = [
        ["test.tag.1", ts[0], r], # range 14:03:00 - 03:29
        ["test.tag.2", ts[1], r],
        ["test.tag.1", ts[2], r],
        ["test.tag.1", ts[3], r], # range 14:03:30 - 04:00
        ["test.tag.1", ts[4], r],
        ["test.tag.1", ts[5], r],
        ["test.tag.1", ts[6], r],
        ["test.tag.1", ts[7], r],
        ["test.tag.2", ts[8], r],
        ["test.tag.1", ts[9], r], # range 14:04:00 - 04:29
        ["test.tag.2", ts[10], r],
      ]

      assert_equal 0, @i.write_count

      @i.interrupt_flushes

      events.shuffle.each do |tag, time, record|
        @i.emit(tag, [ [time, record] ])
      end
      assert{ @i.buffer.stage.size == 3 }
      assert{ @i.write_count == 0 }

      @i.enqueue_thread_wait

      waiting(4) do
        Thread.pass until @i.write_count > 0
      end

      assert{ @i.buffer.stage.size == 2 && @i.write_count == 1 }

      Timecop.freeze( Time.parse('2016-04-13 14:04:04 +0900') )

      @i.enqueue_thread_wait

      assert{ @i.buffer.stage.size == 2 && @i.write_count == 1 }

      Timecop.freeze( Time.parse('2016-04-13 14:04:06 +0900') )

      @i.enqueue_thread_wait
      waiting(4) do
        Thread.pass until @i.write_count > 1
      end

      assert{ @i.buffer.stage.size == 1 && @i.write_count == 2 }

      Timecop.freeze( Time.parse('2016-04-13 14:04:13 +0900') )

      assert_equal 9, ary.size

      @i.stop
      @i.before_shutdown
      @i.shutdown
      @i.after_shutdown

      waiting(4) do
        Thread.pass until @i.write_count > 2
      end

      assert_equal 11, ary.size
      assert metachecks.all?{|e| e }
    end
  end

  sub_test_case 'buffered output feature with tag key' do
    setup do
      chunk_key = 'tag'
      hash = {
        'flush_interval' => 10,
        'flush_threads' => 1,
        'flush_burst_interval' => 0.1,
        'chunk_bytes_limit' => 1024,
      }
      @i = create_output(:buffered)
      @i.configure(config_element('ROOT','',{},[config_element('buffer',chunk_key,hash)]))
      @i.start
    end

    test 'default flush_mode is set to :fast' do
      assert_equal :fast, @i.instance_eval{ @flush_mode }
    end

    test '#start creates enqueue thread and flush threads' do
      @i.thread_wait_until_start

      assert @i.thread_exist?(:flush_thread_0)
      assert @i.thread_exist?(:enqueue_thread)
    end

    test '#format is called for each event streams' do
      ary = []
      @i.register(:format){|tag, time, record| ary << [tag, time, record]; '' }

      t = event_time()
      es = [ [t, {"key" => "value1"}], [t, {"key" => "value2"}] ]

      5.times do
        @i.emit('tag.test', es)
      end

      assert_equal 10, ary.size
      5.times do |i|
        assert_equal ["tag.test", t, {"key" => "value1"}], ary[i*2]
        assert_equal ["tag.test", t, {"key" => "value2"}], ary[i*2+1]
      end
    end

    test '#write is called per tags, per flush_interval & chunk sizes, and buffer chunk is purged' do
      Timecop.freeze( Time.parse('2016-04-13 14:04:01 +0900') )

      ary = []
      metachecks = []

      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:write){|chunk| chunk.read.split("\n").reject{|l| l.empty? }.each{|data| e = JSON.parse(data); ary << e; metachecks << (chunk.metadata.tag == e[0]) } }

      @i.thread_wait_until_start

      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      ts = [
        event_time('2016-04-13 14:03:21 +0900'), event_time('2016-04-13 14:03:23 +0900'), event_time('2016-04-13 14:03:29 +0900'),
        event_time('2016-04-13 14:03:30 +0900'), event_time('2016-04-13 14:03:33 +0900'), event_time('2016-04-13 14:03:38 +0900'),
        event_time('2016-04-13 14:03:43 +0900'), event_time('2016-04-13 14:03:49 +0900'), event_time('2016-04-13 14:03:51 +0900'),
        event_time('2016-04-13 14:04:00 +0900'), event_time('2016-04-13 14:04:01 +0900'),
      ]
      # size of a event is 197
      events = [
        ["test.tag.1", ts[0], r],
        ["test.tag.2", ts[1], r],
        ["test.tag.1", ts[2], r],
        ["test.tag.1", ts[3], r],
        ["test.tag.1", ts[4], r],
        ["test.tag.1", ts[5], r],
        ["test.tag.1", ts[6], r],
        ["test.tag.1", ts[7], r],
        ["test.tag.2", ts[8], r],
        ["test.tag.1", ts[9], r],
        ["test.tag.2", ts[10], r],
      ]

      assert_equal 0, @i.write_count

      @i.interrupt_flushes

      events.shuffle.each do |tag, time, record|
        @i.emit(tag, [ [time, record] ])
      end
      assert{ @i.buffer.stage.size == 2 } # test.tag.1 x1, test.tag.2 x1

      Timecop.freeze( Time.parse('2016-04-13 14:04:02 +0900') )

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      waiting(4) do
        Thread.pass until @i.write_count > 0
      end

      assert{ @i.buffer.stage.size == 2 }
      assert{ @i.write_count == 1 }
      assert{ @i.buffer.queue.size == 0 }

      # events fulfills a chunk (and queued immediately)
      assert_equal 5, ary.size
      assert_equal 5, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 0, ary.select{|e| e[0] == "test.tag.2" }.size

      Timecop.freeze( Time.parse('2016-04-13 14:04:09 +0900') )

      @i.enqueue_thread_wait

      assert{ @i.buffer.stage.size == 2 }

      # to trigger try_flush with flush_burst_interval
      Timecop.freeze( Time.parse('2016-04-13 14:04:11 +0900') )
      @i.enqueue_thread_wait
      Timecop.freeze( Time.parse('2016-04-13 14:04:15 +0900') )
      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      assert{ @i.buffer.stage.size == 0 }

      waiting(4) do
        Thread.pass until @i.write_count > 1
      end

      assert{ @i.buffer.stage.size == 0 && @i.write_count == 3 }

      assert_equal 11, ary.size
      assert_equal 8, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 3, ary.select{|e| e[0] == "test.tag.2" }.size

      assert metachecks.all?{|e| e }
    end

    test 'flush_at_shutdown work well when plugin is shutdown' do
      Timecop.freeze( Time.parse('2016-04-13 14:04:01 +0900') )

      ary = []
      metachecks = []

      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:write){|chunk| chunk.read.split("\n").reject{|l| l.empty? }.each{|data| e = JSON.parse(data); ary << e; metachecks << (chunk.metadata.tag == e[0]) } }

      @i.thread_wait_until_start

      r = {}
      (0...10).each do |i|
        r["key#{i}"] = "value #{i}"
      end
      ts = [
        event_time('2016-04-13 14:03:21 +0900'), event_time('2016-04-13 14:03:23 +0900'), event_time('2016-04-13 14:03:29 +0900'),
        event_time('2016-04-13 14:03:30 +0900'), event_time('2016-04-13 14:03:33 +0900'), event_time('2016-04-13 14:03:38 +0900'),
        event_time('2016-04-13 14:03:43 +0900'), event_time('2016-04-13 14:03:49 +0900'), event_time('2016-04-13 14:03:51 +0900'),
        event_time('2016-04-13 14:04:00 +0900'), event_time('2016-04-13 14:04:01 +0900'),
      ]
      # size of a event is 197
      events = [
        ["test.tag.1", ts[0], r],
        ["test.tag.2", ts[1], r],
        ["test.tag.1", ts[2], r],
        ["test.tag.1", ts[3], r],
        ["test.tag.1", ts[4], r],
        ["test.tag.1", ts[5], r],
        ["test.tag.1", ts[6], r],
        ["test.tag.1", ts[7], r],
        ["test.tag.2", ts[8], r],
        ["test.tag.1", ts[9], r],
        ["test.tag.2", ts[10], r],
      ]

      assert_equal 0, @i.write_count

      @i.interrupt_flushes

      events.shuffle.each do |tag, time, record|
        @i.emit(tag, [ [time, record] ])
      end
      assert{ @i.buffer.stage.size == 2 } # test.tag.1 x1, test.tag.2 x1

      Timecop.freeze( Time.parse('2016-04-13 14:04:02 +0900') )

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      waiting(4) do
        Thread.pass until @i.write_count > 0
      end

      assert{ @i.buffer.stage.size == 2 }
      assert{ @i.write_count == 1 }
      assert{ @i.buffer.queue.size == 0 }

      # events fulfills a chunk (and queued immediately)
      assert_equal 5, ary.size
      assert_equal 5, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 0, ary.select{|e| e[0] == "test.tag.2" }.size

      @i.stop
      @i.before_shutdown
      @i.shutdown
      @i.after_shutdown

      waiting(4) do
        Thread.pass until @i.write_count > 1
      end

      assert{ @i.buffer.stage.size == 0 && @i.buffer.queue.size == 0 && @i.write_count == 3 }

      assert_equal 11, ary.size
      assert_equal 8, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 3, ary.select{|e| e[0] == "test.tag.2" }.size

      assert metachecks.all?{|e| e }
    end
  end

  sub_test_case 'buffered output feature with variables' do
    setup do
      chunk_key = 'name,service'
      hash = {
        'flush_interval' => 10,
        'flush_threads' => 1,
        'flush_burst_interval' => 0.1,
        'chunk_bytes_limit' => 1024,
      }
      @i = create_output(:buffered)
      @i.configure(config_element('ROOT','',{},[config_element('buffer',chunk_key,hash)]))
      @i.start
    end

    test 'default flush_mode is set to :fast' do
      assert_equal :fast, @i.instance_eval{ @flush_mode }
    end

    test '#start creates enqueue thread and flush threads' do
      @i.thread_wait_until_start

      assert @i.thread_exist?(:flush_thread_0)
      assert @i.thread_exist?(:enqueue_thread)
    end

    test '#format is called for each event streams' do
      ary = []
      @i.register(:format){|tag, time, record| ary << [tag, time, record]; '' }

      t = event_time()
      es = [
        [t, {"key" => "value1", "name" => "moris", "service" => "a"}],
        [t, {"key" => "value2", "name" => "moris", "service" => "b"}],
      ]

      5.times do
        @i.emit('tag.test', es)
      end

      assert_equal 10, ary.size
      5.times do |i|
        assert_equal ["tag.test", t, {"key" => "value1", "name" => "moris", "service" => "a"}], ary[i*2]
        assert_equal ["tag.test", t, {"key" => "value2", "name" => "moris", "service" => "b"}], ary[i*2+1]
      end
    end

    test '#write is called per value combination of variables, per flush_interval & chunk sizes, and buffer chunk is purged' do
      Timecop.freeze( Time.parse('2016-04-13 14:04:01 +0900') )

      ary = []
      metachecks = []

      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:write){|chunk| chunk.read.split("\n").reject{|l| l.empty? }.each{|data| e = JSON.parse(data); ary << e; metachecks << (e[2]["name"] == chunk.metadata.variables[:name] && e[2]["service"] == chunk.metadata.variables[:service]) } }

      @i.thread_wait_until_start

      # size of a event is 195
      dummy_data = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
      events = [
        ["test.tag.1", event_time('2016-04-13 14:03:21 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1) xxx-a (6 events)
        ["test.tag.2", event_time('2016-04-13 14:03:23 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}], #(2) yyy-a (3 events)
        ["test.tag.1", event_time('2016-04-13 14:03:29 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.1", event_time('2016-04-13 14:03:30 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.1", event_time('2016-04-13 14:03:33 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.1", event_time('2016-04-13 14:03:38 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "b"}], #(3) xxx-b (2 events)
        ["test.tag.1", event_time('2016-04-13 14:03:43 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.1", event_time('2016-04-13 14:03:49 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "b"}], #(3)
        ["test.tag.2", event_time('2016-04-13 14:03:51 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}], #(2)
        ["test.tag.1", event_time('2016-04-13 14:04:00 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.2", event_time('2016-04-13 14:04:01 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}], #(2)
      ]

      assert_equal 0, @i.write_count

      @i.interrupt_flushes

      events.shuffle.each do |tag, time, record|
        @i.emit(tag, [ [time, record] ])
      end
      assert{ @i.buffer.stage.size == 3 }

      Timecop.freeze( Time.parse('2016-04-13 14:04:02 +0900') )

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      waiting(4) do
        Thread.pass until @i.write_count > 0
      end

      assert{ @i.buffer.stage.size == 3 }
      assert{ @i.write_count == 1 }
      assert{ @i.buffer.queue.size == 0 }

      # events fulfills a chunk (and queued immediately)
      assert_equal 5, ary.size
      assert_equal 5, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 0, ary.select{|e| e[0] == "test.tag.2" }.size
      assert ary[0...5].all?{|e| e[2]["name"] == "xxx" && e[2]["service"] == "a" }

      Timecop.freeze( Time.parse('2016-04-13 14:04:09 +0900') )

      @i.enqueue_thread_wait

      assert{ @i.buffer.stage.size == 3 }

      # to trigger try_flush with flush_burst_interval
      Timecop.freeze( Time.parse('2016-04-13 14:04:11 +0900') )
      @i.enqueue_thread_wait
      Timecop.freeze( Time.parse('2016-04-13 14:04:12 +0900') )
      @i.enqueue_thread_wait
      Timecop.freeze( Time.parse('2016-04-13 14:04:13 +0900') )
      @i.enqueue_thread_wait
      Timecop.freeze( Time.parse('2016-04-13 14:04:14 +0900') )
      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      assert{ @i.buffer.stage.size == 0 }

      waiting(4) do
        Thread.pass until @i.write_count > 1
      end

      assert{ @i.buffer.stage.size == 0 && @i.write_count == 4 }

      assert_equal 11, ary.size
      assert_equal 8, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 3, ary.select{|e| e[0] == "test.tag.2" }.size
      assert_equal 6, ary.select{|e| e[2]["name"] == "xxx" && e[2]["service"] == "a" }.size
      assert_equal 3, ary.select{|e| e[2]["name"] == "yyy" && e[2]["service"] == "a" }.size
      assert_equal 2, ary.select{|e| e[2]["name"] == "xxx" && e[2]["service"] == "b" }.size

      assert metachecks.all?{|e| e }
    end

    test 'flush_at_shutdown work well when plugin is shutdown' do
      Timecop.freeze( Time.parse('2016-04-13 14:04:01 +0900') )

      ary = []
      metachecks = []

      @i.register(:format){|tag,time,record| [tag,time,record].to_json + "\n" }
      @i.register(:write){|chunk| chunk.read.split("\n").reject{|l| l.empty? }.each{|data| e = JSON.parse(data); ary << e; metachecks << (e[2]["name"] == chunk.metadata.variables[:name] && e[2]["service"] == chunk.metadata.variables[:service]) } }

      @i.thread_wait_until_start

      # size of a event is 195
      dummy_data = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
      events = [
        ["test.tag.1", event_time('2016-04-13 14:03:21 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1) xxx-a (6 events)
        ["test.tag.2", event_time('2016-04-13 14:03:23 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}], #(2) yyy-a (3 events)
        ["test.tag.1", event_time('2016-04-13 14:03:29 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.1", event_time('2016-04-13 14:03:30 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.1", event_time('2016-04-13 14:03:33 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.1", event_time('2016-04-13 14:03:38 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "b"}], #(3) xxx-b (2 events)
        ["test.tag.1", event_time('2016-04-13 14:03:43 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.1", event_time('2016-04-13 14:03:49 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "b"}], #(3)
        ["test.tag.2", event_time('2016-04-13 14:03:51 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}], #(2)
        ["test.tag.1", event_time('2016-04-13 14:04:00 +0900'), {"data" => dummy_data, "name" => "xxx", "service" => "a"}], #(1)
        ["test.tag.2", event_time('2016-04-13 14:04:01 +0900'), {"data" => dummy_data, "name" => "yyy", "service" => "a"}], #(2)
      ]

      assert_equal 0, @i.write_count

      @i.interrupt_flushes

      events.shuffle.each do |tag, time, record|
        @i.emit(tag, [ [time, record] ])
      end
      assert{ @i.buffer.stage.size == 3 }

      Timecop.freeze( Time.parse('2016-04-13 14:04:02 +0900') )

      @i.enqueue_thread_wait
      @i.flush_thread_wakeup

      waiting(4) do
        Thread.pass until @i.write_count > 0
      end

      assert{ @i.buffer.stage.size == 3 }
      assert{ @i.write_count == 1 }
      assert{ @i.buffer.queue.size == 0 }

      # events fulfills a chunk (and queued immediately)
      assert_equal 5, ary.size
      assert_equal 5, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 0, ary.select{|e| e[0] == "test.tag.2" }.size

      @i.stop
      @i.before_shutdown
      @i.shutdown
      @i.after_shutdown

      waiting(4) do
        Thread.pass until @i.write_count > 1
      end

      assert{ @i.buffer.stage.size == 0 && @i.buffer.queue.size == 0 && @i.write_count == 4 }

      assert_equal 11, ary.size
      assert_equal 8, ary.select{|e| e[0] == "test.tag.1" }.size
      assert_equal 3, ary.select{|e| e[0] == "test.tag.2" }.size
      assert_equal 6, ary.select{|e| e[2]["name"] == "xxx" && e[2]["service"] == "a" }.size
      assert_equal 3, ary.select{|e| e[2]["name"] == "yyy" && e[2]["service"] == "a" }.size
      assert_equal 2, ary.select{|e| e[2]["name"] == "xxx" && e[2]["service"] == "b" }.size

      assert metachecks.all?{|e| e }
    end
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
