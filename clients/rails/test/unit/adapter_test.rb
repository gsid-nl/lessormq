# frozen_string_literal: true

require "test_helper"

class AdapterTest < ActiveSupport::TestCase
  include LessorMQTestHelpers

  PUBLISHED = {
    "id" => "m42", "queue" => "mail", "status" => "pending", "created_at" => "2030-01-01T00:00:00Z",
    "available_at" => "2030-01-01T00:00:00Z", "duplicate" => false
  }.freeze

  # ActiveJob's test helper puts this adapter on every job, as it does its test adapter.
  include ActiveJob::TestHelper

  def queue_adapter_for_test
    @fake = FakeTransport.new.on("/v1/queues/mail/messages", [201, PUBLISHED])
                         .on("/v1/queues/default/messages", [201, PUBLISHED.merge("queue" => "default")])
                         .on("/v1/queues/closed/messages", [403,
                                                            { "code" => "forbidden", "detail" => "no publish scope" }])
    @adapter = ActiveJob::QueueAdapters::LessormqAdapter.new(fake_client(@fake))
  end

  test "the adapter is found by its name" do
    assert_instance_of ActiveJob::QueueAdapters::LessormqAdapter, ActiveJob::QueueAdapters.lookup(:lessormq).new
    assert_kind_of ActiveJob::QueueAdapters::AbstractAdapter, @adapter
    assert_same @adapter, RecordingJob.queue_adapter
  end

  test "perform_later publishes the serialized job to the queue of its queue_name" do
    job = RecordingJob.perform_later("ada@example.com", { "template" => "welcome" })

    assert job.successfully_enqueued?
    assert_equal "m42", job.provider_job_id
    sent = @fake.calls("/v1/queues/mail/messages").first
    assert_equal "RecordingJob", sent["body"]["job_class"]
    assert_equal job.job_id, sent["body"]["job_id"]
    assert_equal "ada@example.com", sent["body"]["arguments"].first
    assert_equal({ "type" => "RecordingJob" }, sent["headers"])
    assert_match(/\Almq-auto-/, sent["dedup_key"])
    refute sent.key?("delay_seconds")
    refute sent.key?("max_attempts")
  end

  test "a scheduled job gets a delay in whole seconds, rounded up" do
    RecordingJob.set(wait: 10.minutes).perform_later
    RecordingJob.set(wait: 1.5.seconds).perform_later
    RecordingJob.set(wait_until: 1.hour.ago).perform_later

    first, second, third = @fake.calls("/v1/queues/mail/messages")
    assert_includes 599..600, first["delay_seconds"]
    assert_equal 2, second["delay_seconds"]
    refute third.key?("delay_seconds")
  end

  test "set(queue:) picks the LessorMQ queue; priority is not sent" do
    RecordingJob.set(queue: "default", priority: 5).perform_later

    sent = @fake.calls("/v1/queues/default/messages").first
    assert_equal "RecordingJob", sent["body"]["job_class"]
    refute sent.key?("priority")
  end

  test "lessormq_options set max_attempts and a dedup key" do
    @fake.on("/v1/queues/default/messages", [200, PUBLISHED.merge("duplicate" => true)])

    LimitedJob.perform_later(7)

    sent = @fake.calls("/v1/queues/default/messages").first
    assert_equal 3, sent["max_attempts"]
    assert_equal "limited-7", sent["dedup_key"]
    assert_equal({}, RecordingJob.lessormq_publish_options)
  end

  test "a failed publish is an EnqueueError, so perform_later returns false" do
    refute RecordingJob.set(queue: "closed").perform_later
    job = RecordingJob.new.set(queue: "closed")
    refute job.enqueue
    assert_instance_of ActiveJob::EnqueueError, job.enqueue_error
    assert_equal "LessorMQ: no publish scope", job.enqueue_error.message
  end

  test "perform_all_later publishes every job" do
    ActiveJob.perform_all_later(RecordingJob.new(1), RecordingJob.new(2).set(wait: 30))

    sent = @fake.calls("/v1/queues/mail/messages")
    assert_equal([[1], [2]], sent.map { |s| s["body"]["arguments"] })
    assert_includes 29..30, sent.last["delay_seconds"]
  end

  test "the client comes from the configuration by default" do
    config = LessorMQ::Rails::Configuration.new
    config.url = "https://mq.test"
    config.api_key = "lmq_secret"

    assert_instance_of LessorMQ::Client, config.client
    assert_same config.client, config.client
    assert_equal "lessormq-rails/#{LessorMQ::Rails::VERSION} lessormq-ruby/#{LessorMQ::VERSION}",
                 config.client.instance_variable_get(:@user_agent)
    refute_includes config.inspect, "lmq_secret"
    assert_raises(ArgumentError) { LessorMQ::Rails::Configuration.new.tap { |c| c.url = nil }.client }
  end
end
