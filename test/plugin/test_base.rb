require_relative '../helper'
require 'fluent/plugin/base'

class DummyPlugin < Fluent::Plugin::Base
end

class BaseTest < Test::Unit::TestCase
  setup do
    @p = DummyPlugin.new
  end

  test 'has methods for phases of plugin life cycle, and methods to know "super"s were correctly called or not' do
    assert !@p.configured?
    @p.configure(config_element())
    assert @p.configured?

    assert !@p.started?
    @p.start
    assert @p.start

    assert !@p.stopped?
    @p.stop
    assert @p.stopped?

    assert !@p.before_shutdown?
    @p.before_shutdown
    assert @p.before_shutdown?

    assert !@p.shutdown?
    @p.shutdown
    assert @p.shutdown?

    assert !@p.after_shutdown?
    @p.after_shutdown
    assert @p.after_shutdown?

    assert !@p.closed?
    @p.close
    assert @p.closed?

    assert !@p.terminated?
    @p.terminate
    assert @p.terminated?
  end

  test 'can access system config' do
    assert @p.system_config

    @p.system_config_override({'process_name' => 'mytest'})
    assert_equal 'mytest', @p.system_config.process_name
  end

  test 'does not have router in default' do
    assert !@p.has_router?
  end

  test 'is configurable by config_param and config_section' do
    assert_nothing_raised do
      class DummyPlugin2 < Fluent::Plugin::TestBase
        config_param :myparam1, :string
        config_section :mysection, multi: false do
          config_param :myparam2, :integer
        end
      end
    end
    p2 = DummyPlugin2.new
    assert_nothing_raised do
      p2.configure(config_element('ROOT', '', {'myparam1' => 'myvalue1'}, [config_element('mysection', '', {'myparam2' => 99})]))
    end
    assert_equal 'myvalue1', p2.myparam1
    assert_equal 99, p2.mysection.myparam2
  end
end
