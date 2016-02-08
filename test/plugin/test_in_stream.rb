require_relative '../helper'
require 'fluent/test'
require 'fluent/plugin/in_stream'

module StreamInputTest
  def setup
    Fluent::Test.setup
  end

  def test_time
    d = create_driver

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")
    Fluent::Engine.now = time

    records = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}],
    ]

    d.run do
      records.each do |tag,time,record|
        send_data pack_event(tag, 0, record)
      end
    end
    assert_equal records, d.emits
  end

  def test_message
    d = create_driver

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    records = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}],
    ]

    d.run do
      records.each do |tag,time,record|
        send_data pack_event(tag, time, record)
      end
    end
    assert_equal records, d.emits
  end

  def test_forward
    d = create_driver

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    records = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}],
    ]

    d.run do
      entries = []

      records.each do |tag,time,record|
        entries << [time, record]
      end
      send_data pack_event("tag1", entries)
    end
    assert_equal records, d.emits
  end

  def test_packed_forward
    d = create_driver

    time = Fluent::EventTime.parse("2011-01-02 13:14:15 UTC")

    records = [
      ["tag1", time, {"a"=>1}],
      ["tag1", time, {"a"=>2}],
    ]

    d.run do
      entries = ''
      records.each do |tag,time,record|
        entries << pack_event(time, record)
      end
      send_data pack_event("tag1", entries)
    end
    assert_equal records, d.emits
  end

  def test_message_json
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    records = [
      ["tag1", time, {"a"=>1}],
      ["tag2", time, {"a"=>2}],
    ]

    d.run do
      records.each do |tag,time,record|
        send_data [tag, time, record].to_json
      end
    end
    assert_equal records, d.emits
  end

  def create_driver(klass, conf)
    Fluent::Test::InputTestDriver.new(klass).configure(conf)
  end

  def send_data(data)
    io = connect
    begin
      io.write data
    ensure
      io.close
    end
  end
end

class UnixInputTest < Test::Unit::TestCase
  include StreamInputTest

  TMP_DIR = File.dirname(__FILE__) + "/../tmp/in_unix#{ENV['TEST_ENV_NUMBER']}"
  CONFIG = %[
    path #{TMP_DIR}/unix
    backlog 1000
  ]

  def create_driver(conf=CONFIG)
    super(Fluent::UnixInput, conf)
  end

  def test_configure
    d = create_driver
    assert_equal "#{TMP_DIR}/unix", d.instance.path
    assert_equal 1000, d.instance.backlog
  end

  def connect
    UNIXSocket.new("#{TMP_DIR}/unix")
  end
end unless Fluent.windows?
