# frozen_string_literal: true

# End to end: jobs dispatched through ActiveJob in a Rails app, run by the worker against a
# LessorMQ server. Needs LESSORMQ_URL and LESSORMQ_OPERATOR_KEY; every run makes a tenant
# of its own and deletes it afterwards.

require "json"
require "net/http"
require "securerandom"
require "minitest/autorun"

URL = ENV.fetch("LESSORMQ_URL") { abort "LESSORMQ_URL and LESSORMQ_OPERATOR_KEY are needed" }
OPERATOR_KEY = ENV.fetch("LESSORMQ_OPERATOR_KEY") { abort "LESSORMQ_OPERATOR_KEY is needed" }

# The admin API, for what the test sets up and checks.
module Admin
  module_function

  def call(method, path, key, body = nil, tenant: nil)
    uri = URI.join(URL, path)
    request = Net::HTTP.const_get(method.capitalize).new(uri)
    request["Authorization"] = "Bearer #{key}"
    request["LessorMQ-Tenant"] = tenant if tenant
    if body
      request["Content-Type"] = "application/json"
      request.body = JSON.generate(body)
    end
    response = Net::HTTP.start(uri.host, uri.port) { |http| http.request(request) }
    raise "#{method} #{path}: #{response.code} #{response.body}" unless response.code.start_with?("2")

    response.body.to_s.empty? ? nil : JSON.parse(response.body)
  end
end

TENANT = "rails-e2e-#{SecureRandom.hex(4)}".freeze
TENANT_KEY = Admin.call(:post, "/v1/tenants", OPERATOR_KEY, { id: TENANT, name: TENANT }).dig("admin_key", "secret")
Minitest.after_run do
  Admin.call(:patch, "/v1/tenants/#{TENANT}", OPERATOR_KEY, { status: "suspended" })
  Admin.call(:delete, "/v1/tenants/#{TENANT}", OPERATOR_KEY)
end

ENV["LESSORMQ_KEY"] = TENANT_KEY
$LOAD_PATH.unshift(File.expand_path("../../lib", __dir__))
require_relative "../dummy/config/environment"

# What reaches Rails' error reporter.
module Reported
  @errors = []

  def self.errors
    @errors
  end

  def self.report(error, **)
    @errors << error
  end
end
Rails.error.subscribe(Reported)

class WorkerTest < Minitest::Test
  def fetch_message(id)
    Admin.call(:get, "/v1/messages/#{id}", TENANT_KEY)
  end

  def settings(queue, patch)
    Admin.call(:patch, "/v1/queues/#{queue}", TENANT_KEY, patch)
  end

  def work(queue, max_messages: 1, wait_seconds: 2)
    LessorMQ::Rails::Runner.new(queues: [queue], max_messages: max_messages, wait_seconds: wait_seconds)
                           .run(handle_signals: false)
  end

  def test_a_dispatched_job_is_run_by_the_worker_and_acked
    job = GreetingJob.perform_later("Ada")

    assert job.successfully_enqueued?
    assert_equal({ "type" => "GreetingJob" }, fetch_message(job.provider_job_id)["headers"])
    assert_equal 1, work("default")
    done = fetch_message(job.provider_job_id)
    assert_equal "completed", done["status"]
    assert_equal 1, done["attempts"]
  end

  def test_a_failing_job_comes_back_after_the_queues_backoff_with_the_servers_attempt_count
    settings("flaky", { max_attempts: 2, backoff: { initial_seconds: 2, jitter: 0 } })
    FlakyJob.executions_seen.clear
    Reported.errors.clear
    job = FlakyJob.perform_later

    work("flaky")
    after_first = fetch_message(job.provider_job_id)
    assert_equal "pending", after_first["status"]
    assert_equal 1, after_first["attempts"]
    assert_equal "IOError", after_first.dig("last_error", "type")
    waited = Time.iso8601(after_first["available_at"]) - Time.iso8601(after_first["updated_at"])
    assert_in_delta 2, waited, 1
    # retry_on enqueued no copy: the queue holds this one message.
    stats = Admin.call(:get, "/v1/queues/flaky", TENANT_KEY)["stats"]
    assert_equal 1, stats["ready"] + stats["delayed"]

    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    work("flaky", wait_seconds: 5)
    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :>=, 1

    assert_equal [1, 2], FlakyJob.executions_seen
    assert_equal(%w[IOError IOError], Reported.errors.map { |e| e.class.name })
    dead = fetch_message(job.provider_job_id)
    assert_equal "failed", dead["status"]
    assert_equal 2, dead["attempts"]
  end

  def test_a_discarded_job_goes_to_the_dead_letter_set_at_once
    Reported.errors.clear
    job = UnusableJob.perform_later

    work("unusable")
    assert_empty Reported.errors

    dead = fetch_message(job.provider_job_id)
    assert_equal "failed", dead["status"]
    assert_equal 1, dead["attempts"]
    assert_equal 7, dead["max_attempts"]
    assert_equal "ArgumentError", dead.dig("last_error", "type")
    assert_equal "cannot be decoded", dead.dig("last_error", "message")
  end

  def test_the_worker_command_runs_jobs_until_term
    job = GreetingJob.set(queue: "command").perform_later("Grace")
    env = { "LESSORMQ_URL" => URL, "LESSORMQ_KEY" => TENANT_KEY, "QUEUES" => "command", "DUMMY_LOG" => "/dev/stderr" }
    rakefile = File.expand_path("../dummy/Rakefile", __dir__)
    pid = Process.spawn(env, Gem.ruby, Gem.bin_path("rake", "rake"), "-f", rakefile, "lessormq:work")

    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 30
    sleep 0.2 until fetch_message(job.provider_job_id)["status"] == "completed" ||
                    Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
    assert_equal "completed", fetch_message(job.provider_job_id)["status"]

    Process.kill("TERM", pid)
    stopped_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    _, status = Process.wait2(pid)
    assert status.success?, "the worker exited with #{status.inspect}"
    # A claim that was waiting ends within wait_seconds (2 in the dummy app).
    assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - stopped_at, :<, 5
  ensure
    if pid && !status
      Process.kill("KILL", pid)
      Process.wait(pid)
    end
  end
end
