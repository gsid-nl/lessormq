# frozen_string_literal: true

require "test_helper"

class ClientTest < Minitest::Test
  def client(fake, **options)
    LessorMQ::Client.new("https://mq.test/", "lmq_key", retry_delay: 0, transport: fake, **options)
  end

  def test_publish_sends_the_body_and_an_automatic_dedup_key
    fake = FakeTransport.new.on("/v1/queues/mail/messages", [201, PUBLISHED])

    published = client(fake).publish("mail", { to: "ada@example.com" }, headers: { "type" => "Welcome" },
                                                                        max_attempts: 3)

    sent = fake.calls("/v1/queues/mail/messages").first
    assert_equal({ "to" => "ada@example.com" }, sent["body"])
    assert_equal({ "type" => "Welcome" }, sent["headers"])
    assert_equal 3, sent["max_attempts"]
    assert_match(/\Almq-auto-\h{24}\z/, sent["dedup_key"])
    refute sent.key?("delay_seconds")
    assert_equal "m1", published.id
    refute published.duplicate?
    assert_equal Time.utc(2030, 1, 1), published.available_at
    request = fake.requests.first
    assert_equal "Bearer lmq_key", request.headers["Authorization"]
    assert_equal "application/json", request.headers["Content-Type"]
    assert_equal "lessormq-ruby/#{LessorMQ::VERSION}", request.headers["User-Agent"]
  end

  def test_publish_keeps_null_bodies_encodes_the_queue_and_formats_times_in_utc
    fake = FakeTransport.new.on("/v1/queues/q%2F1%20x/messages", [201, PUBLISHED])
    at = Time.new(2030, 1, 1, 4, 0, 0, "+02:00")

    client(fake).publish("q/1 x", nil, dedup_key: "k1", available_at: at)

    sent = fake.calls("/v1/queues/q%2F1%20x/messages").first
    assert_equal({ "body" => nil, "dedup_key" => "k1", "available_at" => "2030-01-01T02:00:00Z" }, sent)
  end

  def test_publish_without_a_dedup_key_is_not_retried
    fake = FakeTransport.new.on("/v1/queues/mail/messages", [503, { "code" => "internal" }], [201, PUBLISHED])

    assert_raises(LessorMQ::ServerError) { client(fake, auto_dedup_key: false).publish("mail", {}) }

    assert_equal 1, fake.requests.size
  end

  def test_publish_with_a_dedup_key_is_retried_and_may_be_a_duplicate
    fake = FakeTransport.new.on("/v1/queues/mail/messages", LessorMQ::TransportError.new("reset"),
                                [200, PUBLISHED.merge("duplicate" => true)])

    published = client(fake).publish("mail", {}, dedup_key: "order-7", delay_seconds: 60)

    assert published.duplicate?
    assert_equal 2, fake.requests.size
    assert_equal({ "body" => {}, "dedup_key" => "order-7", "delay_seconds" => 60 },
                 fake.calls("/v1/queues/mail/messages").last)
  end

  def test_claim_returns_nil_when_nothing_arrived
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [204, nil])

    assert_nil client(fake).claim("mail", wait_seconds: 5, route: "vpn-1", consumer: "w1")
    assert_equal({ "consumer" => "w1", "route" => "vpn-1", "wait_seconds" => 5 },
                 fake.calls("/v1/queues/mail/claims").first)
  end

  def test_claim_waits_longer_than_the_wait
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [204, nil])

    client(fake, timeout: 3).claim("mail", wait_seconds: 20)

    assert_equal 23, fake.requests.first.timeout
  end

  def test_claim_returns_the_lease
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, LEASE])

    lease = client(fake).claim("mail")

    assert_equal "r1", lease.receipt
    assert_equal({ "to" => "ada@example.com" }, lease.message.body)
    assert_equal({ "type" => "Welcome" }, lease.message.headers)
    assert_equal 1, lease.message.attempt
    refute lease.message.last_attempt?
    assert_equal Time.utc(2030, 1, 1, 0, 0, 30), lease.lease_until
    refute_includes lease.inspect, "r1"
  end

  def test_extend_lease_moves_the_lease
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, LEASE])
                        .on("/v1/messages/m1/extend", [200, { "lease_until" => "2030-01-01T00:05:00Z" }])
    c = client(fake)
    lease = c.claim("mail")

    c.extend_lease(lease, lease_seconds: 300)

    assert_equal({ "receipt" => "r1", "lease_seconds" => 300 }, fake.calls("/v1/messages/m1/extend").first)
    assert_equal Time.utc(2030, 1, 1, 0, 5), lease.lease_until
  end

  def test_fail_reports_the_exception
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, LEASE]).on("/v1/messages/m1/fail", [200, FAIL_RESULT])
    c = client(fake)
    lease = c.claim("mail")

    result = begin
      raise Timeout::Error, "mail server slow"
    rescue Timeout::Error => e
      c.fail(lease, e)
    end

    sent = fake.calls("/v1/messages/m1/fail").first
    assert_equal "r1", sent["receipt"]
    assert_equal false, sent["permanent"]
    assert_equal "Timeout::Error", sent["error"]["type"]
    assert_equal "mail server slow", sent["error"]["message"]
    assert sent["error"]["details"]["file"].end_with?("client_test.rb")
    assert_kind_of Integer, sent["error"]["details"]["line"]
    assert_equal LessorMQ::FailResult.new(outcome: "retrying", available_at: Time.utc(2030, 1, 1, 0, 0, 10),
                                          attempt: 1, max_attempts: 7), result
  end

  class BadInput < ArgumentError
    include LessorMQ::Permanent
  end

  class Unusable < StandardError
    def permanent?
      true
    end
  end

  def test_fail_marks_permanent_failures
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, LEASE]).on("/v1/messages/m1/fail", [200, FAIL_RESULT])
    c = client(fake)
    lease = c.claim("mail")

    c.fail(lease, BadInput.new("no recipient"))
    c.fail(lease, Unusable.new(""))
    c.fail(lease, LessorMQ::PermanentFailure.new("bad"), permanent: false)
    c.fail(lease, "plain text", details: { "step" => 2 }, type: "Custom")

    first, second, third, fourth = fake.calls("/v1/messages/m1/fail")
    assert_equal true, first["permanent"]
    assert_equal "ClientTest::BadInput", first["error"]["type"]
    refute first["error"].key?("details")
    assert_equal true, second["permanent"]
    assert_equal "(no message)", second["error"]["message"]
    assert_equal false, third["permanent"]
    assert_equal({ "receipt" => "r1", "error" => { "message" => "plain text", "type" => "Custom",
                                                   "details" => { "step" => 2 } }, "permanent" => false }, fourth)
  end

  def test_fail_cuts_long_messages_and_repairs_bad_encodings
    fake = FakeTransport.new.on("/v1/messages/m1/fail", [200, FAIL_RESULT])
    lease = LessorMQ::Lease.from_api(LEASE)

    client(fake).fail(lease, "x" * 70_000)
    client(fake).fail(lease, "caf\xE9".b)

    long, binary = fake.calls("/v1/messages/m1/fail")
    assert_equal 65_535, long["error"]["message"].length
    assert_equal "caf\uFFFD", binary["error"]["message"]
  end

  def test_lease_lost_is_raised
    fake = FakeTransport.new.on("/v1/messages/m1/ack",
                                [409, { "code" => "lease_lost", "detail" => "The lease ended." }])

    error = assert_raises(LessorMQ::LeaseLost) { client(fake).ack(LessorMQ::Lease.from_api(LEASE)) }

    assert_equal 409, error.status
    assert_equal "lease_lost", error.code
    assert_equal "The lease ended.", error.message
    assert_equal 1, fake.requests.size
  end

  def test_server_errors_and_network_errors_are_retried
    fake = FakeTransport.new.on("/v1/queues/mail/claims", LessorMQ::TransportError.new("refused"),
                                [502, "<html>Bad gateway</html>"], [200, LEASE])

    refute_nil client(fake).claim("mail")
    assert_equal 3, fake.requests.size
  end

  def test_retries_run_out
    fake = FakeTransport.new.on("/v1/queues/mail/claims", LessorMQ::TransportError.new("refused"))

    error = assert_raises(LessorMQ::TransportError) { client(fake, http_retries: 2).claim("mail") }

    assert_equal 3, fake.requests.size
    assert_includes error.message, "POST /v1/queues/mail/claims"
  end

  def test_rate_limits_wait_for_retry_after
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [429, { "code" => "rate_limited" }, { "retry-after" => "0" }])

    error = assert_raises(LessorMQ::RateLimited) { client(fake, http_retries: 1).claim("mail") }

    assert_equal 0, error.retry_after
    assert_equal 2, fake.requests.size
  end

  def test_validation_errors_carry_the_fields
    errors = [{ "field" => "lease_seconds", "message" => "too long" }]
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [400, { "code" => "validation_failed", "errors" => errors }])

    error = assert_raises(LessorMQ::ValidationFailed) { client(fake).claim("mail", lease_seconds: 99_999) }

    assert_equal errors, error.errors
    assert_equal 1, fake.requests.size
  end

  def test_errors_without_a_problem_body_get_a_code_from_the_status
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [403, "Forbidden"])

    error = assert_raises(LessorMQ::Forbidden) { client(fake).claim("mail") }

    assert_equal "forbidden", error.code
    assert_kind_of LessorMQ::ApiError, error
    assert_kind_of LessorMQ::Error, error
  end

  def test_release
    fake = FakeTransport.new.on("/v1/messages/m1/release", [204, nil])

    client(fake).release(LessorMQ::Lease.from_api(LEASE), delay_seconds: 30)
    client(fake).release(LessorMQ::Lease.from_api(LEASE))

    assert_equal [{ "receipt" => "r1", "delay_seconds" => 30 }, { "receipt" => "r1" }],
                 fake.calls("/v1/messages/m1/release")
  end

  def test_claim_batch_and_ack_batch
    second = LEASE.merge("receipt" => "r2")
    fake = FakeTransport.new
                        .on("/v1/queues/mail/claims", [200, { "items" => [LEASE, second] }], [204, nil])
                        .on("/v1/acks", [200, { "items" => [{ "message" => "m1", "outcome" => "acked" },
                                                            { "message" => "m1", "outcome" => "lease_lost" }] }])
    c = client(fake)

    leases = c.claim_batch("mail", 10, wait_seconds: 2)
    outcomes = c.ack_batch(leases)

    assert_equal %w[r1 r2], leases.map(&:receipt)
    assert_equal({ "wait_seconds" => 2, "max_messages" => 10 }, fake.calls("/v1/queues/mail/claims").first)
    assert_equal %w[acked lease_lost], outcomes
    assert_equal [{ "message" => "m1", "receipt" => "r1" }, { "message" => "m1", "receipt" => "r2" }],
                 fake.calls("/v1/acks").first["leases"]
    assert_equal [], c.claim_batch("mail", 10)
  end

  def test_stats
    stats = { "ready" => 3, "delayed" => 1, "processing" => 2, "completed" => 9, "failed" => 1,
              "oldest_ready_seconds" => 12, "oldest_processing_seconds" => 4, "new_count" => 5 }
    fake = FakeTransport.new.on("/v1/queues/mail", [200, { "name" => "mail", "stats" => stats }])

    result = client(fake).stats("mail")

    assert_equal 3, result.ready
    assert_equal 1, result.failed
    assert_equal 12, result.oldest_ready_seconds
    assert_equal({ "new_count" => 5 }, result.other)
  end

  def test_unknown_fields_are_ignored
    lease = LEASE.merge("new_field" => 1, "message" => LEASE["message"].merge("started_at" => "2030-01-01T00:00:01Z"))
    fake = FakeTransport.new.on("/v1/queues/mail/claims", [200, lease])

    assert_equal "m1", client(fake).claim("mail").message.id
  end

  def test_the_key_stays_out_of_inspect
    refute_includes client(FakeTransport.new).inspect, "lmq_key"
    refute_includes client(FakeTransport.new).to_s, "lmq_key"
  end
end
