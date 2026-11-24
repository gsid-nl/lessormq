# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "json"
require "logger"
require "minitest/autorun"
require "active_support"
require "active_support/test_case"
require "active_job"
require "active_job/test_helper"
require "lessormq/rails"
# The scripted server of the client's own tests.
require_relative "../../ruby/test/fake_transport"

ActiveJob::Base.logger = Logger.new(nil)
GlobalID.app = "lessormq-test"
ActiveSupport.test_order = :random if ActiveSupport.respond_to?(:test_order=)

require_relative "support/jobs"

module LessorMQTestHelpers
  def fake_client(fake)
    LessorMQ::Client.new("https://mq.test", "k", retry_delay: 0, transport: fake)
  end

  # A message as the worker gets it: the job's serialized form, through JSON.
  def message_for(job, attempt: 1, id: "m1")
    body = JSON.parse(JSON.generate(job.serialize))
    LessorMQ::Message.new(id: id, queue: job.queue_name, body: body, headers: { "type" => job.class.name },
                          attempt: attempt, max_attempts: 5, created_at: Time.now.utc)
  end

  def lease_for(message)
    LessorMQ::Lease.new("r1", Time.now.utc + 300, message)
  end

  def lease_body(job, attempt: 1)
    {
      "receipt" => "r1", "lease_until" => (Time.now.utc + 300).iso8601,
      "message" => {
        "id" => "m1", "queue" => job.queue_name, "body" => JSON.parse(JSON.generate(job.serialize)),
        "headers" => {}, "attempt" => attempt, "max_attempts" => 5, "created_at" => Time.now.utc.iso8601
      }
    }
  end
end
