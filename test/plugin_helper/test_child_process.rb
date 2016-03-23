require_relative '../helper'
require 'fluent/plugin_helper/child_process'
require 'fluent/plugin/base'
require 'timeout'

class ChildProcessTest < Test::Unit::TestCase
  TEST_DEADLOCK_TIMEOUT = 30

  setup do
    @d = Dummy.new
    @d.configure(config_element())
    @d.start
  end

  teardown do
    if @d
      @d.stop      unless @d.stopped?
      @d.shutdown  unless @d.shutdown?
      @d.close     unless @d.closed?
      @d.terminate unless @d.terminated?
    end
  end

  class Dummy < Fluent::Plugin::Base
    helpers :child_process
    def configure(conf)
      super
      @_child_process_kill_timeout = 1
    end
  end

  test 'can be instantiated' do
    d1 = Dummy.new
    assert d1.respond_to?(:_child_process_processes)
  end

  test 'can be configured and started' do
    d1 = Dummy.new
    assert_nothing_raised do
      d1.configure(config_element())
    end
    assert d1.plugin_id
    assert d1.log

    d1.start
  end

  test 'can execute external command asyncronously' do
    m = Mutex.new
    m.lock
    ary = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      @d.child_process_execute(:t0, 'echo', arguments: ['foo', 'bar'], mode: [:read]) do |io|
        m.lock
        ran = true
        io.read # discard
        ary << 2
        m.unlock
      end
      ary << 1
      m.unlock
      sleep 0.1 until m.locked? || ran
      m.lock
      m.unlock
    end
    assert_equal [1,2], ary
  end

  test 'can execute external command at just once, which finishes immediately' do
    m = Mutex.new
    t1 = Time.now
    ary = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      @d.child_process_execute(:t1, 'echo', arguments: ['foo', 'bar'], mode: [:read]) do |io|
        m.lock
        ran = true
        ary << io.read
        assert io.eof?
        m.unlock
      end
      sleep 0.1 until m.locked? || ran
      m.lock
      m.unlock
    end
    assert{ Time.now - t1 < 4.0 }
  end

  test 'can execute external command at just once, which can handle both of read and write' do
    m = Mutex.new
    t1 = Time.now
    ary = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      cmd = "ruby -e 'while !STDIN.eof? && line = STDIN.readline; puts line.chomp; STDOUT.flush rescue nil; end'"
      @d.child_process_execute(:t2, cmd, mode: [:write, :read]) do |writeio, readio|
        m.lock
        ran = true

        [[1,2],[3,4],[5,6]].each do |i,j|
          writeio.write "my data#{i}\n"
          writeio.write "my data#{j}\n"
          writeio.flush
        end
        writeio.close

        while line = readio.readline
          ary << line
        end
        assert readio.eof?
        readio.close

        m.unlock
      end
      sleep 0.1 until m.locked? || ran
      m.lock
      m.unlock
    end

    assert_equal [], @d.log.out.logs
    expected = (1..6).map{|i| "my data#{i}\n" }
    assert_equal expected, ary
  end

  test 'can execute external command at just once, which runs forever' do
    m = Mutex.new
    t1 = Time.now
    ary = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      @d.child_process_execute(:t3, "ruby -e 'while sleep 0.01; puts 1; STDOUT.flush rescue nil; end'", mode: [:read]) do |io|
        m.lock
        ran = true
        begin
          while @d.child_process_running? && line = io.readline
            ary << line
          end
        rescue
          # ignore
        ensure
          m.unlock
        end
      end
      sleep 0.1 until m.locked? || ran
      sleep 0.5
      @d.stop # nothing occures
      @d.shutdown

      assert{ ary.size > 5 }

      @d.close

      @d.terminate
      assert @d._child_process_processes.empty?
    end
  end

  test 'can execute external command just once, and can terminate it forcedly when shutdown/terminate even if it ignore SIGTERM' do
    m = Mutex.new
    t1 = Time.now
    ary = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      @d.child_process_execute(:t4, "ruby -e 'Signal.trap(:TERM, nil); while sleep 0.01; puts 1; STDOUT.flush rescue nil; end'", mode: [:read]) do |io|
        m.lock
        ran = true
        begin
          while line = io.readline
            ary << line
          end
        rescue
          # ignore
        ensure
          m.unlock
        end
      end
      sleep 0.1 until m.locked? || ran

      assert_equal [], @d.log.out.logs

      @d.stop # nothing occures
      sleep 0.5
      lines1 = ary.size
      assert{ lines1 > 1 }

      pid = @d._child_process_processes.keys.first

      @d.shutdown
      sleep 0.5
      lines2 = ary.size
      assert{ lines2 > lines1 }

      @d.close

      assert_nil((Process.waitpid(pid, Process::WNOHANG) rescue nil))

      @d.terminate
      assert @d._child_process_processes.empty?
      begin
        Process.waitpid(pid)
      rescue Errno::ECHILD
      end
      # Process successfully KILLed if test reaches here
      assert true
    end
  end

  test 'can execute external command many times, which finishes immediately' do
    ary = []
    arguments = ['-e', '3.times{ puts "okay"; STDOUT.flush rescue nil; sleep 0.01 }']
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      @d.child_process_execute(:t5, "ruby", arguments: arguments, interval: 0.8, mode: [:read]) do |io|
        ary << io.read.split("\n").map(&:chomp).join
      end
      sleep 4
      assert_equal [], @d.log.out.logs
      @d.stop
      assert_equal [], @d.log.out.logs
      @d.shutdown; @d.close; @d.terminate
      assert{ ary.size >= 3 && ary.size <= 5 }
    end
  end

  test 'can execute external command many times, with leading once executed immediately' do
    ary = []
    arguments = ['-e', '3.times{ puts "okay"; STDOUT.flush rescue nil; sleep 0.01 }']
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      @d.child_process_execute(:t6, "ruby", arguments: arguments, interval: 0.8, immediate: true, mode: [:read]) do |io|
        ary << io.read.split("\n").map(&:chomp).join
      end
      sleep 4
      @d.stop; @d.shutdown; @d.close; @d.terminate
      assert{ ary.size >= 3 && ary.size <= 6 }
      assert_equal [], @d.log.out.logs
    end
  end

  test 'does not execute long running external command in parallel in default' do
    ary = []
    arguments = ['-e', '100.times{ puts "okay"; STDOUT.flush rescue nil; sleep 0.1 }'] # 10 sec
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      @d.child_process_execute(:t7, "ruby", arguments: arguments, interval: 1, immediate: true, mode: [:read]) do |io|
        ary << io.read.split("\n").map(&:chomp).join
      end
      sleep 4
      assert_equal 1, @d._child_process_processes.size
      @d.stop
      warn_msg = '[warn]: previous child process is still running. skipped. title=:t7 command="ruby" arguments=["-e", "100.times{ puts \\"okay\\"; STDOUT.flush rescue nil; sleep 0.1 }"] interval=1 parallel=false' + "\n"
      assert{ @d.log.out.logs.first.end_with?(warn_msg) }
      assert{ @d.log.out.logs.all?{|line| line.end_with?(warn_msg) } }
      @d.shutdown; @d.close; @d.terminate
      assert_equal [], @d.log.out.logs
    end
  end

  test 'can execute long running external command in parallel if specified' do
    ary = []
    arguments = ['-e', '100.times{ puts "okay"; STDOUT.flush rescue nil; sleep 0.1 }'] # 10 sec
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      @d.child_process_execute(:t8, "ruby", arguments: arguments, interval: 1, immediate: true, parallel: true, mode: [:read]) do |io|
        ary << io.read.split("\n").map(&:chomp).join
      end
      sleep 4
      processes = @d._child_process_processes.size
      assert{ processes >= 3 && processes <= 5 }
      @d.stop; @d.shutdown; @d.close; @d.terminate
      assert_equal [], @d.log.out.logs
    end
  end

  test 'execute external processes only for writing' do
    m = Mutex.new
    ary = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      @d.child_process_execute(:t9, "ruby", arguments: ['-e', 'a=""; while b=STDIN.readline; a+=b; end'], mode: [:write]) do |io|
        m.lock
        ran = true
        unreadable = false
        begin
          io.read
        rescue IOError
          unreadable = true
        end
        assert unreadable

        50.times do
          io.write "hahaha\n"
        end
        m.unlock
      end
      sleep 0.1 until m.locked? || ran
      m.lock
      m.unlock
      @d.stop; @d.shutdown; @d.close; @d.terminate
      assert_equal [], @d.log.out.logs
    end
  end

  test 'execute external processes only for reading' do
    m = Mutex.new
    ary = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      @d.child_process_execute(:t10, "ruby", arguments: ['-e', 'while sleep 0.01; puts 1; STDOUT.flush rescue nil; end'], mode: [:read]) do |io|
        m.lock
        ran = true
        unwritable = false
        begin
          io.write "foobar"
        rescue IOError
          unwritable = true
        end
        assert unwritable

        data = io.readline

        m.unlock
      end
      sleep 0.1 until m.locked? || ran
      m.lock
      m.unlock
      @d.stop; @d.shutdown; @d.close; @d.terminate
      assert_equal [], @d.log.out.logs
    end
  end

  test 'can control external encodings' do
    m = Mutex.new
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      @d.child_process_execute(:t11, "ruby -e 'sleep 10'", external_encoding: 'ascii-8bit') do |r, w|
        m.lock
        ran = true
        assert Encoding::ASCII_8BIT, r.external_encoding
        assert Encoding::ASCII_8BIT, w.external_encoding
        m.unlock
      end
      sleep 0.1 until m.locked? || ran
      m.lock
      assert true
      @d.stop; @d.shutdown; @d.close; @d.terminate
    end
  end

  test 'can control internal encodings' do
    m = Mutex.new
    encodings = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      @d.child_process_execute(:t12, "ruby -e 'sleep 10'", internal_encoding: 'ascii-8bit') do |r, w|
        m.lock
        assert_equal Encoding::ASCII_8BIT, r.internal_encoding
        assert_equal Encoding::ASCII_8BIT, w.internal_encoding
        ran = true
        m.unlock
      end
      sleep 0.1 until m.locked? || ran
      m.lock
      assert true
      @d.stop; @d.shutdown; @d.close; @d.terminate
    end
  end

  test 'can convert encodings' do
    m = Mutex.new
    encodings = []
    Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
      ran = false
      args = ['-e', 'STDOUT.set_encoding("ascii-8bit"); STDOUT.write  "\xA4\xB5\xA4\xC8\xA4\xB7"']
      @d.child_process_execute(:t13, "ruby", arguments: args, external_encoding: 'euc-jp', internal_encoding: 'windows-31j', mode: [:read]) do |io|
        m.lock
        str = io.read
        assert_equal Encoding.find('windows-31j'), str.encoding

        expected = "さとし".encoding('windows-31j')
        assert_equal expected, str
        ran = true
        m.unlock
      end
      sleep 0.1 until m.locked? || ran
      m.lock
      assert true
      @d.stop; @d.shutdown; @d.close; @d.terminate
    end
  end

  unless Fluent.windows?
    test 'can specify subprocess name' do
      io = IO.popen([["cat", "caaaaaaaaaaat"], '-'])
      process_naming_enabled = (open("|ps"){|io| io.readlines }.select{|line| line.include?("caaaaaaaaaaat") }.size > 0)
      Process.kill(:TERM, io.pid) rescue nil
      io.close rescue nil

      # Does TravisCI prohibit process renaming?
      # This test will be passed in such environment
      pend unless process_naming_enabled

      m = Mutex.new
      pids = []
      proc_lines = []
      Timeout.timeout(TEST_DEADLOCK_TIMEOUT) do
        ran = false
        @d.child_process_execute(:t14, "ruby", arguments:['-e', 'sleep 10'], subprocess_name: "sleeeeeeeeeper", mode: [:read]) do |readio|
          m.lock
          assert readio
          pids << @d.child_process_id
          proc_lines += open("|ps"){|io| io.readlines }
          ran = true
          m.unlock
        end
        sleep 0.1 until m.locked? || ran
        m.lock
        pid = pids.first
        # 51358 ttys001    0:00.00 sleeper -e sleep 10
        assert{ proc_lines.select{|line| line =~ /^\s*#{pid}\s/ }.first.strip.split(/\s+/)[3] == "sleeeeeeeeeper" }
        @d.stop; @d.shutdown; @d.close; @d.terminate
      end
    end
  end
end