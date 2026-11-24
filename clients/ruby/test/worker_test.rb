# frozen_string_literal: true

require "test_helper"

class WorkerTest < Minitest::Test
  def client(fake)
    LessorMQ::Client.new("https://mq.test", "k", retry_delay: 0, transport: fake)
  end

  def worker(fake, handler = nil, **options, &)
    options = { handle_signals: false, max_messages: 1, consumer: "w1", wait_seconds: 1 }.merge(options)
    LessorMQ::Worker.new(client(fake), "mail", handler, **options, &)
  end

  def test_a_handler_that_returns_acks
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, LEASE]).on("/v1/messages/m1/ack", [204, nil])
    seen = []

    handled = worker(fake) { |message, job| seen << [message, job] }.run

    assert_equal 1, handled
    message, job = seen.first
    assert_equal({ "to" => "ada@example.com" }, message.body)
    assert_equal "r1", job.lease.receipt
    assert_same message, job.message
    assert_equal [{ "receipt" => "r1" }], fake.calls("/v1/messages/m1/ack")
    assert_equal({ "consumer" => "w1", "wait_seconds" => 1 }, fake.calls("/v1/queues/mail/claims").first)
  end

  def test_a_callable_is_a_handler_too
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, LEASE]).on("/v1/messages/m1/ack", [204, nil])
    calls = []

    worker(fake, ->(message, _job) { calls << message.id }).run

    assert_equal ["m1"], calls
  end

  def test_a_handler_that_raises_fails_and_the_worker_goes_on
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, LEASE]).on("/v1/messages/m1/fail", [200, FAIL_RESULT])

    handled = worker(fake, max_messages: 2) { raise LessorMQ::PermanentFailure, "no recipient" }.run

    assert_equal 2, handled
    sent = fake.calls("/v1/messages/m1/fail")
    assert_equal 2, sent.size
    assert_equal true, sent.first["permanent"]
    assert_equal "LessorMQ::PermanentFailure", sent.first["error"]["type"]
    assert_equal "no recipient", sent.first["error"]["message"]
  end

  def test_errors_beyond_standard_error_fail_too
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, LEASE]).on("/v1/messages/m1/fail", [200, FAIL_RESULT])

    worker(fake) { raise NotImplementedError, "later" }.run

    assert_equal "NotImplementedError", fake.calls("/v1/messages/m1/fail").first["error"]["type"]
  end

  def test_the_lease_is_extended_while_the_handler_runs
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, LEASE])
                        .on("/v1/messages/m1/extend", [200, { "lease_until" => "2030-01-01T00:01:00Z" }])
                        .on("/v1/messages/m1/ack", [204, nil])

    worker(fake, extend_every: 0.1, lease_seconds: 60) { sleep 0.35 }.run

    extends = fake.calls("/v1/messages/m1/extend")
    assert_includes 2..4, extends.size
    assert_equal({ "receipt" => "r1", "lease_seconds" => 60 }, extends.first)
    assert_equal 1, fake.calls("/v1/messages/m1/ack").size
  end

  def test_a_failed_extend_is_tried_again
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, LEASE])
                        .on("/v1/messages/m1/extend", [400, { "code" => "validation_failed" }],
                            [200, { "lease_until" => "2030-01-01T00:01:00Z" }])
                        .on("/v1/messages/m1/ack", [204, nil])

    worker(fake, extend_every: 0.1) { sleep 0.35 }.run

    assert_operator fake.calls("/v1/messages/m1/extend").size, :>=, 2
    assert_equal 1, fake.calls("/v1/messages/m1/ack").size
  end

  def test_a_lost_lease_is_not_reported
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, LEASE])
                        .on("/v1/messages/m1/extend", [409, { "code" => "lease_lost" }])
    lost = nil

    worker(fake, extend_every: 0.1) do |_message, job|
      sleep 0.25
      lost = job.lease_lost?
    end.run

    assert lost
    assert_equal 1, fake.calls("/v1/messages/m1/extend").size
    assert_empty fake.calls("/v1/messages/m1/ack")
    assert_empty fake.calls("/v1/messages/m1/fail")
  end

  def test_no_extends_without_auto_extend
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, LEASE]).on("/v1/messages/m1/ack", [204, nil])

    worker(fake, extend_every: 0.05, auto_extend: false) { sleep 0.2 }.run

    assert_empty fake.calls("/v1/messages/m1/extend")
  end

  def test_a_lost_ack_is_survived
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, LEASE])
                        .on("/v1/messages/m1/ack", [409, { "code" => "lease_lost" }])

    assert_equal 2, worker(fake, max_messages: 2) { nil }.run
  end

  def test_a_claim_failure_pauses_and_goes_on
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [503, { "code" => "internal" }], [200, LEASE])
                        .on("/v1/messages/m1/ack", [204, nil])
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    handled = LessorMQ::Worker.new(
      LessorMQ::Client.new("https://mq.test", "k", retry_delay: 0, http_retries: 0, transport: fake), "mail",
      handle_signals: false, max_messages: 1, error_pause: 0.2
    ) { nil }.run

    assert_equal 1, handled
    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :>=, 0.2
  end

  def test_stop_while_the_claim_waits_releases_the_message
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, LEASE]).on("/v1/messages/m1/release", [204, nil])
    ran = false
    w = worker(fake, max_messages: nil) { ran = true }
    fake.define_singleton_method(:request) do |*args|
      w.stop if args[1] == "/v1/queues/mail/claims"
      super(*args)
    end

    assert_equal 0, w.run
    refute ran
    assert_equal [{ "receipt" => "r1" }], fake.calls("/v1/messages/m1/release")
  end

  def test_a_signal_stops_the_worker_after_the_current_message
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, LEASE]).on("/v1/messages/m1/ack", [204, nil])
    previous = Signal.trap("TERM") { flunk "the worker should have trapped TERM" }

    handled = worker(fake, max_messages: nil, handle_signals: true) do
      Process.kill("TERM", Process.pid)
      sleep 0.1
    end.run

    assert_equal 1, handled
    assert_equal 1, fake.calls("/v1/messages/m1/ack").size
  ensure
    Signal.trap("TERM", previous || "DEFAULT")
  end

  def test_concurrency_runs_handlers_side_by_side_without_passing_max_messages
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, LEASE])
                        .on("/v1/messages/m1/ack", [204, nil]).on("/v1/messages/m1/release", [204, nil])
    running = 0
    peak = 0
    lock = Mutex.new

    handled = worker(fake, concurrency: 3, max_messages: 6) do
      lock.synchronize { peak = [peak, running += 1].max }
      sleep 0.1
      lock.synchronize { running -= 1 }
    end.run

    assert_equal 6, handled
    assert_equal 3, peak
    assert_equal 6, fake.calls("/v1/messages/m1/ack").size
  end

  def test_a_worker_stopped_before_it_runs_claims_nothing
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, LEASE])
    w = worker(fake, max_messages: nil) { nil }

    w.stop

    assert_equal 0, w.run
    assert_empty fake.requests
    assert w.stopping?
  end

  def test_max_seconds_stops_claiming
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [204, nil])

    assert_equal 0, worker(fake, max_messages: nil, max_seconds: 0.1) { nil }.run
  end

  def test_a_handler_is_required
    assert_raises(ArgumentError) { LessorMQ::Worker.new(client(FakeTransport.new), "mail") }
  end

  def test_the_default_consumer_is_host_and_pid
    assert_equal "#{Socket.gethostname}:#{Process.pid}", LessorMQ::Worker.default_consumer
  end
end
