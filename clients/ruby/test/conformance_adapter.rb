# frozen_string_literal: true

# Runs the conformance scenarios through this library (conformance/php/src/ProcessAdapter.php).
#
# One JSON request per line on stdin, one JSON result per line on stdout. Every call goes
# through LessorMQ::Client, and its results and errors are turned back into the
# {status, body} the scenarios check:
#
#   LESSORMQ_ADAPTER_CMD="ruby clients/ruby/test/conformance_adapter.rb" php conformance/php/run.php

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "json"
require "lessormq"

module ConformanceAdapter
  URL = ENV.fetch("LESSORMQ_URL", "http://127.0.0.1:18080")

  module_function

  def clients
    @clients ||= Hash.new { |hash, key| hash[key] = LessorMQ::Client.new(URL, key, retry_delay: 0.05) }
  end

  def call(name, api_key, args)
    client = clients[api_key]
    case name
    when "publish" then publish(client, args)
    when "claim" then claim(client, args)
    when "claim_batch" then claim_batch(client, args)
    when "extend"
      lease = client.extend_lease(lease_of(args), lease_seconds: args["lease_seconds"])
      { status: 200, body: { lease_until: LessorMQ::Times.format(lease.lease_until) } }
    when "ack"
      client.ack(lease_of(args))
      { status: 204, body: nil }
    when "ack_batch"
      items = args["leases"] || []
      outcomes = client.ack_batch(items.map { |item| lease_of(item) })
      { status: 200, body: { items: items.zip(outcomes).map { |item, outcome|
        { message: item["message"], outcome: outcome }
      } } }
    when "fail" then fail_message(client, args)
    when "release"
      client.release(lease_of(args), delay_seconds: args["delay_seconds"] || 0)
      { status: 204, body: nil }
    else raise ArgumentError, "unknown call #{name}"
    end
  rescue LessorMQ::ApiError => e
    { status: e.status, body: { code: e.code, detail: e.detail, errors: e.errors } }
  end

  def publish(client, args)
    available_at = args["available_at"]
    published = client.publish(
      args["queue"], args["body"],
      headers: args["headers"],
      dedup_key: args["dedup_key"],
      delay_seconds: args["delay_seconds"],
      available_at: available_at ? LessorMQ::Times.parse(available_at) : nil,
      max_attempts: args["max_attempts"]
    )
    {
      status: published.duplicate? ? 200 : 201,
      body: {
        id: published.id, queue: published.queue, status: published.status,
        created_at: LessorMQ::Times.format(published.created_at),
        available_at: LessorMQ::Times.format(published.available_at), duplicate: published.duplicate?
      }
    }
  end

  def claim(client, args)
    lease = client.claim(args["queue"], **claim_options(args))
    lease ? { status: 200, body: lease_body(lease) } : { status: 204, body: nil }
  end

  def claim_batch(client, args)
    leases = client.claim_batch(args["queue"], Integer(args["max_messages"]), **claim_options(args))
    leases.empty? ? { status: 204, body: nil } : { status: 200, body: { items: leases.map { |l| lease_body(l) } } }
  end

  def claim_options(args)
    {
      consumer: args["consumer"], route: args["route"], wait_seconds: args["wait_seconds"] || 0,
      lease_seconds: args["lease_seconds"]
    }
  end

  def lease_body(lease)
    message = lease.message
    {
      receipt: lease.receipt,
      lease_until: LessorMQ::Times.format(lease.lease_until),
      message: {
        id: message.id, queue: message.queue, body: message.body, headers: message.headers,
        attempt: message.attempt, max_attempts: message.max_attempts,
        created_at: LessorMQ::Times.format(message.created_at)
      }
    }
  end

  def fail_message(client, args)
    error = args["error"] || {}
    result = client.fail(
      lease_of(args), error["message"].to_s,
      permanent: args["permanent"] ? true : false, details: error["details"], type: error["type"]
    )
    {
      status: 200,
      body: {
        outcome: result.outcome,
        available_at: result.available_at && LessorMQ::Times.format(result.available_at),
        attempt: result.attempt, max_attempts: result.max_attempts
      }
    }
  end

  # A lease as the scenario names it: the message id and the receipt, which may be stale
  # or someone else's on purpose.
  def lease_of(args)
    now = Time.now.utc
    message = LessorMQ::Message.new(id: args["message"].to_s, queue: "", body: nil, headers: {}, attempt: 0,
                                    max_attempts: 0, created_at: now)
    LessorMQ::Lease.new(args["receipt"].to_s, now, message)
  end

  def main
    $stdout.sync = true
    $stdin.each_line do |line|
      next if line.strip.empty?

      request = JSON.parse(line)
      args = request["args"].is_a?(Hash) ? request["args"] : {}
      $stdout.puts(JSON.generate(call(request["call"], request["api_key"], args)))
    end
  end
end

ConformanceAdapter.main if $PROGRAM_NAME == __FILE__
