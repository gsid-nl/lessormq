# frozen_string_literal: true

require "test_helper"

# The executor runs a job as the worker does. ActiveJob's test adapter is in place
# (ActiveJob::TestHelper), so anything a job enqueues instead of failing shows up.
class ExecutorTest < ActiveSupport::TestCase
  include ActiveJob::TestHelper
  include LessorMQTestHelpers

  setup { Runs.clear }

  def run_job(job, attempt: 1)
    message = message_for(job, attempt: attempt)
    lease_job = LessorMQ::Job.new(fake_client(FakeTransport.new), lease_for(message))
    LessorMQ::Rails::Executor.new.call(message, lease_job)
  end

  test "a job that returns is done; it sees the server's attempt as executions" do
    run_job(RecordingJob.new("ada", { "n" => 1 }), attempt: 3)

    run = Runs.all.first
    assert_equal ["ada", { "n" => 1 }], run[:args]
    assert_equal 3, run[:executions]
    assert_equal "m1", run[:provider_job_id]
    assert_equal "r1", run[:lease]
  end

  test "an exception fails the message, not permanent, and nothing is enqueued" do
    error = assert_raises(IOError) { run_job(FailingJob.new, attempt: 2) }

    assert_equal "upstream slow", error.message
    refute LessorMQ.permanent?(error)
    assert_equal 2, Runs.all.first[:executions]
    assert_no_enqueued_jobs
  end

  test "retry_on fails the message with its error instead of enqueueing a copy" do
    5.times do |i|
      error = assert_raises(IOError) { run_job(RetryOnJob.new, attempt: i + 1) }
      refute LessorMQ.permanent?(error)
    end

    assert_equal([1, 2, 3, 4, 5], Runs.all.map { |run| run[:executions] })
    assert_no_enqueued_jobs
  end

  test "a retry_on that gives up (attempts: 1, with a block) is a permanent failure" do
    error = assert_raises(IOError) { run_job(GiveUpJob.new) }

    assert LessorMQ.permanent?(error)
    assert_equal([:give_up_block], Runs.all.map { |run| run[:job] })
    assert_no_enqueued_jobs
  end

  test "retry_job from a rescue_from fails the message with the error" do
    error = assert_raises(IOError) { run_job(RetryJobJob.new) }

    assert_equal "try later", error.message
    assert_no_enqueued_jobs
  end

  test "retry_job without an error fails the message too" do
    error = assert_raises(LessorMQ::Rails::RetryRequested) { run_job(ManualRetryJob.new) }

    assert_match(/ManualRetryJob asked to be retried/, error.message)
    assert_no_enqueued_jobs
  end

  test "a job that enqueues itself again is done, and the copy is a new job" do
    run_job(RescheduleJob.new)

    assert_enqueued_jobs 1, only: RescheduleJob
  end

  test "discard_on is a permanent failure with the job's own error; its block and after_discard run" do
    error = assert_raises(ArgumentError) { run_job(DiscardOnJob.new("argument")) }

    assert_equal "bad input", error.message
    assert LessorMQ.permanent?(error)
    assert_equal(%i[discard_block after_discard], Runs.all.map { |run| run[:job] })
  end

  test "after_discard does not run for an error the queue may try again" do
    error = assert_raises(IOError) { run_job(DiscardOnJob.new("io")) }

    refute LessorMQ.permanent?(error)
    assert_empty Runs.all
  end

  test "after_discard runs as ActiveJob says outside the worker" do
    assert_raises(IOError) { DiscardOnJob.perform_now("io") }

    assert_equal([:after_discard], Runs.all.map { |run| run[:job] })
  end

  test "retry_on outside the worker enqueues a copy, as ActiveJob does" do
    RetryOnJob.perform_now

    assert_enqueued_jobs 1, only: RetryOnJob
  end

  test "a LessorMQ::Permanent error stays permanent" do
    error = assert_raises(PermanentJob::Unusable) { run_job(PermanentJob.new) }

    assert LessorMQ.permanent?(error)
  end

  test "arguments that cannot be deserialized are a permanent failure" do
    job = ArgumentJob.new
    message = message_for(job)
    body = message.body.merge("arguments" => [{ "_aj_globalid" => "gid://lessormq-test/MissingModel/1" }])
    message = message.with(body: body)

    error = assert_raises(ActiveJob::DeserializationError) do
      LessorMQ::Rails::Executor.new.call(message, LessorMQ::Job.new(fake_client(FakeTransport.new), lease_for(message)))
    end

    assert LessorMQ.permanent?(error)
    assert_empty Runs.all
  end

  test "retry_on DeserializationError keeps it retryable" do
    message = message_for(RetryDeserializationJob.new)
    missing = { "_aj_globalid" => "gid://lessormq-test/MissingModel/1" }
    message = message.with(body: message.body.merge("arguments" => [missing]))

    error = assert_raises(ActiveJob::DeserializationError) do
      LessorMQ::Rails::Executor.new.call(message, LessorMQ::Job.new(fake_client(FakeTransport.new), lease_for(message)))
    end

    refute LessorMQ.permanent?(error)
    assert_no_enqueued_jobs
  end

  test "a message that is not a job is a permanent failure" do
    message = message_for(RecordingJob.new).with(body: { "to" => "ada@example.com" })

    error = assert_raises(LessorMQ::Rails::InvalidJob) do
      LessorMQ::Rails::Executor.new.call(message, LessorMQ::Job.new(fake_client(FakeTransport.new), lease_for(message)))
    end

    assert LessorMQ.permanent?(error)
  end

  test "an unknown job class is not permanent: another worker may have it" do
    message = message_for(RecordingJob.new)
    message = message.with(body: message.body.merge("job_class" => "NotDeployedYetJob"))

    error = assert_raises(NameError) do
      LessorMQ::Rails::Executor.new.call(message, LessorMQ::Job.new(fake_client(FakeTransport.new), lease_for(message)))
    end

    refute LessorMQ.permanent?(error)
  end
end
