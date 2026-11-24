# frozen_string_literal: true

require "time"

module LessorMQ
  # Times as the API writes them: RFC 3339, UTC, whole seconds.
  module Times
    module_function

    def parse(value)
      Time.iso8601(value.to_s).utc
    end

    def format(time)
      time.getutc.strftime("%Y-%m-%dT%H:%M:%SZ")
    end
  end

  # A delivered message: what a handler runs. +body+ is the JSON value as published
  # (a Hash with string keys, an Array, a String, ...).
  Message = Data.define(:id, :queue, :body, :headers, :attempt, :max_attempts, :created_at) do
    # Whether this attempt is the last one the queue allows.
    def last_attempt?
      attempt >= max_attempts
    end

    def self.from_api(data)
      headers = data["headers"]
      new(
        id: data["id"].to_s,
        queue: data["queue"].to_s,
        body: data["body"],
        headers: headers.is_a?(Hash) ? headers : {},
        attempt: Integer(data["attempt"]),
        max_attempts: Integer(data["max_attempts"]),
        created_at: Times.parse(data["created_at"])
      )
    end
  end

  # A claimed message and the proof of holding it. Extend, ack, fail and release take
  # the lease; only its receipt is accepted, and only until +lease_until+, which
  # Client#extend_lease moves.
  class Lease
    attr_reader :receipt, :message
    attr_accessor :lease_until

    def initialize(receipt, lease_until, message)
      @receipt = receipt
      @lease_until = lease_until
      @message = message
    end

    def seconds_left
      lease_until - Time.now
    end

    def self.from_api(data)
      new(data["receipt"].to_s, Times.parse(data["lease_until"]), Message.from_api(data["message"]))
    end

    # Never the receipt: it is the proof of holding the message.
    def inspect
      "#<LessorMQ::Lease message=#{message.id} lease_until=#{Times.format(lease_until)}>"
    end
  end

  # The answer to a publish: the message, or the open one with the same dedup key
  # (+duplicate?+).
  Published = Data.define(:id, :queue, :status, :created_at, :available_at, :duplicate) do
    alias_method :duplicate?, :duplicate

    def self.from_api(data)
      new(
        id: data["id"].to_s,
        queue: data["queue"].to_s,
        status: data["status"].to_s,
        created_at: Times.parse(data["created_at"]),
        available_at: Times.parse(data["available_at"]),
        duplicate: data["duplicate"] == true
      )
    end
  end

  # What the server did with a failed attempt. For logging: a client never acts on it.
  FailResult = Data.define(:outcome, :available_at, :attempt, :max_attempts) do
    def self.from_api(data)
      available_at = data["available_at"]
      new(
        outcome: data["outcome"].to_s,
        available_at: available_at ? Times.parse(available_at) : nil,
        attempt: Integer(data["attempt"]),
        max_attempts: Integer(data["max_attempts"])
      )
    end
  end

  class FailResult
    RETRYING = "retrying"
    DEAD_LETTERED = "dead_lettered"
    DISCARDED = "discarded"
  end

  # A queue's counts, for monitoring. Not part of the protocol. Counts the server adds
  # later are in +other+.
  Stats = Data.define(
    :ready, :delayed, :processing, :completed, :failed,
    :oldest_ready_seconds, :oldest_processing_seconds, :other
  ) do
    def self.from_api(data)
      known = members - [:other]
      numbers = data.select { |_name, value| value.is_a?(Numeric) }.transform_values(&:to_i)
      values = known.to_h { |name| [name, numbers.fetch(name.to_s, 0)] }
      new(**values, other: numbers.reject { |name, _| known.include?(name.to_sym) })
    end
  end
end
