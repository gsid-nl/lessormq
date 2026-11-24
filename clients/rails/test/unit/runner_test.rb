# frozen_string_literal: true

require "test_helper"

class RunnerTest < ActiveSupport::TestCase
  # Jobs that would enqueue anything go to ActiveJob's test adapter.
  include ActiveJob::TestHelper
  include LessorMQTestHelpers

  setup { Runs.clear }

  def config
    LessorMQ::Rails::Configuration.new.tap do |c|
      c.queues = ["mail"]
      c.wait_seconds = 1
      c.consumer = "w1"
    end
  end

  test "the worker runs a job and acks it" do
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, lease_body(RecordingJob.new("ada"), attempt: 2)])
                        .on("/v1/messages/m1/ack", [204, nil])

    handled = LessorMQ::Rails::Runner.new(client: fake_client(fake), config: config, max_messages: 1)
                                     .run(handle_signals: false)

    assert_equal 1, handled
    assert_equal [{ "consumer" => "w1", "wait_seconds" => 1 }], fake.calls("/v1/queues/mail/claims")
    assert_equal [{ "receipt" => "r1" }], fake.calls("/v1/messages/m1/ack")
    assert_equal 2, Runs.all.first[:executions]
  end

  test "a failing job is failed with its error, a discarded one as permanent" do
    fail_result = { "outcome" => "retrying", "available_at" => "2030-01-01T00:00:10Z", "attempt" => 1,
                    "max_attempts" => 5 }
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, lease_body(RetryOnJob.new)],
                                [200, lease_body(DiscardOnJob.new("argument"))])
                        .on("/v1/messages/m1/fail", [200, fail_result])

    LessorMQ::Rails::Runner.new(client: fake_client(fake), config: config, max_messages: 2).run(handle_signals: false)

    retried, discarded = fake.calls("/v1/messages/m1/fail")
    assert_equal({ "type" => "IOError", "message" => "upstream slow", "permanent" => false },
                 { "type" => retried["error"]["type"], "message" => retried["error"]["message"],
                   "permanent" => retried["permanent"] })
    assert_equal "ArgumentError", discarded["error"]["type"]
    assert_equal true, discarded["permanent"]
    assert_no_enqueued_jobs
  end

  test "QUEUES and THREADS come from the environment" do
    runner = LessorMQ::Rails::Runner.from_env({ "QUEUES" => "mail, default", "THREADS" => "3" },
                                              config: config.tap { |c| c.client = fake_client(FakeTransport.new) })

    assert_equal %w[mail default], runner.workers.map(&:queue)
  end

  test "TERM stops every worker after its current job" do
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, lease_body(RecordingJob.new)])
                        .on("/v1/queues/default/claims", [204, nil])
                        .on("/v1/messages/m1/ack", [204, nil])
    c = config.tap { |cfg| cfg.queues = %w[mail default] }
    runner = LessorMQ::Rails::Runner.new(client: fake_client(fake), config: c)
    previous = Signal.trap("TERM", "DEFAULT")

    thread = Thread.new { runner.run }
    sleep 0.2
    Process.kill("TERM", Process.pid)

    assert thread.join(5), "the runner did not stop"
    assert_operator thread.value, :>=, 1
    assert_equal "DEFAULT", Signal.trap("TERM", previous)
  end
end
