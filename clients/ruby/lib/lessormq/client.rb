# frozen_string_literal: true

require "json"
require "securerandom"

module LessorMQ
  # The protocol calls of LessorMQ (https://lessormq.dev/docs/protocol), nothing more.
  #
  # The client never decides what happens to a message: no retry of a failed job, no
  # delay, no dead-lettering. It reports, and the queue decides. It does retry its own
  # HTTP calls when that is safe: network errors, 5xx and 429, for every call that can be
  # repeated (all of them, given the automatic dedup key on publish).
  #
  # Thread-safe: a worker's keepalive thread shares it.
  class Client
    USER_AGENT = "lessormq-ruby/1"
    MAX_ERROR_MESSAGE = 65_535
    MAX_ERROR_TYPE = 255

    # One protocol call as HTTP: what to send, whether it may be sent again, and how
    # much longer than the timeout it may take.
    Call = Struct.new(:verb, :path, :payload, :repeatable, :extra_timeout)
    Response = Struct.new(:status, :body)
    private_constant :Call, :Response

    # http_retries:: retries after a network error, a 5xx or a 429, for calls that are
    #                safe to repeat (all but a publish without a dedup key)
    # retry_delay::  base delay between retries in seconds, times the attempt number
    # auto_dedup_key:: give every publish without a dedup key one of its own, so a retried
    #                publish can never create a second message
    # timeout::      seconds per request; a claim gets its wait on top
    # transport::    sends the requests; NetHTTPTransport by default
    # logger::       a Logger for the retries; nothing is logged by default
    def initialize(base_url, api_key, http_retries: 3, retry_delay: 0.1, auto_dedup_key: true, timeout: 10,
                   user_agent: USER_AGENT, transport: nil, logger: nil)
      @base_url = base_url.to_s.chomp("/")
      @api_key = api_key
      @http_retries = http_retries
      @retry_delay = retry_delay
      @auto_dedup_key = auto_dedup_key
      @timeout = timeout
      @user_agent = user_agent
      @transport = transport || NetHTTPTransport.new(@base_url)
      @logger = logger
    end

    attr_reader :logger

    # Never the key.
    def inspect
      "#<LessorMQ::Client #{@base_url}>"
    end
    alias to_s inspect

    # Puts a message on a queue. +body+ is any JSON value.
    #
    # With a dedup key (yours, or the automatic one), an open message with that key is
    # returned instead of a new one: Published#duplicate?.
    def publish(queue, body, headers: nil, dedup_key: nil, delay_seconds: nil, available_at: nil, max_attempts: nil)
      dedup_key = "lmq-auto-#{SecureRandom.hex(12)}" if dedup_key.nil? && @auto_dedup_key
      payload = { "body" => body }
      payload["headers"] = headers if headers && !headers.empty?
      payload["dedup_key"] = dedup_key unless dedup_key.nil?
      payload["delay_seconds"] = delay_seconds unless delay_seconds.nil?
      payload["available_at"] = Times.format(available_at) unless available_at.nil?
      payload["max_attempts"] = max_attempts unless max_attempts.nil?
      # Without a dedup key a repeated publish could create a second message.
      call = Call.new("POST", "/v1/queues/#{segment(queue)}/messages", payload, !dedup_key.nil?, 0)
      Published.from_api(send_call(call).body)
    end

    # Leases the next message of a queue, waiting up to +wait_seconds+ (at most 20) for
    # one. Nil when nothing became available, or the queue is paused.
    def claim(queue, consumer: nil, route: nil, wait_seconds: 0, lease_seconds: nil)
      response = send_call(claim_call(queue, consumer, route, wait_seconds, lease_seconds, nil))
      response.status == 204 ? nil : Lease.from_api(response.body)
    end

    # Leases up to +max_messages+ (1 to 100) ready messages at once, oldest first, each
    # with a receipt of its own. Waits up to +wait_seconds+ only while there is none; an
    # empty array when nothing became available. Report each lease as usual, or ack them
    # together with #ack_batch.
    def claim_batch(queue, max_messages, consumer: nil, route: nil, wait_seconds: 0, lease_seconds: nil)
      response = send_call(claim_call(queue, consumer, route, wait_seconds, lease_seconds, max_messages))
      return [] if response.status == 204

      Array(response.body["items"]).map { |item| Lease.from_api(item) }
    end

    # Moves the end of the lease to now plus +lease_seconds+ (or the queue's lease), and
    # returns the lease. (Not +extend+: that is Object#extend.)
    #
    # Raises LeaseLost when the lease already ended.
    def extend_lease(lease, lease_seconds: nil)
      payload = { "receipt" => lease.receipt }
      payload["lease_seconds"] = lease_seconds unless lease_seconds.nil?
      response = send_call(Call.new("POST", message_path(lease, "extend"), payload, true, 0))
      lease.lease_until = Times.parse(response.body["lease_until"])
      lease
    end

    # Reports success. Raises LeaseLost when the lease already ended.
    def ack(lease)
      send_call(Call.new("POST", message_path(lease, "ack"), { "receipt" => lease.receipt }, true, 0))
      nil
    end

    # Reports success for up to 100 leases in one request. The outcomes are in the order
    # of +leases+: "acked" (also when acked before), "lease_lost", "not_found" or
    # "forbidden"; one that cannot be acked does not hold up the others.
    def ack_batch(leases)
      items = leases.map { |lease| { "message" => lease.message.id, "receipt" => lease.receipt } }
      response = send_call(Call.new("POST", "/v1/acks", { "leases" => items }, true, 0))
      outcomes = Array(response.body["items"])
      Array.new(leases.size) { |i| outcomes[i].is_a?(Hash) ? outcomes[i]["outcome"].to_s : "not_found" }
    end

    # Reports failure. The server decides what follows and says so in the FailResult.
    #
    # error::     the exception, or a message
    # permanent:: the input itself cannot be processed; by default whether +error+ is
    #             marked permanent (LessorMQ.permanent?)
    # details::   any JSON value with more context; by default where the exception was raised
    # type::      the error type; by default the exception's class
    #
    # Raises LeaseLost when the lease already ended.
    def fail(lease, error, permanent: nil, details: nil, type: nil)
      if error.is_a?(Exception)
        text = error.message
        type ||= error.class.name || "Exception"
        details ||= origin(error)
        permanent = LessorMQ.permanent?(error) if permanent.nil?
      else
        text = error.to_s
      end
      report = { "message" => utf8(text)[0, MAX_ERROR_MESSAGE] }
      report["message"] = "(no message)" if report["message"].empty?
      report["type"] = utf8(type)[0, MAX_ERROR_TYPE] unless type.nil?
      report["details"] = details unless details.nil?
      payload = { "receipt" => lease.receipt, "error" => report, "permanent" => permanent ? true : false }
      FailResult.from_api(send_call(Call.new("POST", message_path(lease, "fail"), payload, true, 0)).body)
    end

    # Gives the message back without an outcome; the attempt does not count. Raises
    # LeaseLost when the lease already ended.
    def release(lease, delay_seconds: 0)
      payload = { "receipt" => lease.receipt }
      payload["delay_seconds"] = delay_seconds if delay_seconds.positive?
      send_call(Call.new("POST", message_path(lease, "release"), payload, true, 0))
      nil
    end

    # A queue's counts. Needs the consume or admin scope. Not part of the protocol; for
    # monitoring.
    def stats(queue)
      body = send_call(Call.new("GET", "/v1/queues/#{segment(queue)}", nil, true, 0)).body
      Stats.from_api(body.is_a?(Hash) && body["stats"].is_a?(Hash) ? body["stats"] : {})
    end

    private

    def claim_call(queue, consumer, route, wait_seconds, lease_seconds, max_messages)
      payload = {}
      payload["consumer"] = consumer unless consumer.nil?
      payload["route"] = route unless route.nil?
      payload["wait_seconds"] = wait_seconds if wait_seconds.positive?
      payload["lease_seconds"] = lease_seconds unless lease_seconds.nil?
      payload["max_messages"] = max_messages unless max_messages.nil?
      # A claim whose answer got lost leaves a lease that simply expires and counts as an
      # attempt, so claims are retried like the rest.
      Call.new("POST", "/v1/queues/#{segment(queue)}/claims", payload, true, [wait_seconds, 0].max)
    end

    def send_call(call)
      headers = {
        "Authorization" => "Bearer #{@api_key}",
        "Accept" => "application/json",
        "User-Agent" => @user_agent
      }
      body = nil
      unless call.payload.nil?
        headers["Content-Type"] = "application/json"
        body = JSON.generate(call.payload)
      end

      attempt = 0
      loop do
        attempt += 1
        begin
          status, response_headers, raw = @transport.request(
            call.verb, call.path, headers, body, @timeout + call.extra_timeout
          )
        rescue TransportError => e
          unless call.repeatable && attempt <= @http_retries
            raise TransportError, "#{call.verb} #{call.path} failed: #{e.message}"
          end

          pause(attempt, nil, call.path, e.message)
          next
        end

        parsed = parse(raw)
        return Response.new(status, parsed) if status < 400

        retry_after = retry_after(response_headers)
        error = ApiError.from_response(status, parsed, retry_after)
        # A 429 was not processed at all, so it is always safe to send again.
        again = error.is_a?(RateLimited) || (call.repeatable && error.is_a?(ServerError))
        raise error unless again && attempt <= @http_retries

        pause(attempt, retry_after, call.path, error.message)
      end
    end

    def pause(attempt, retry_after, path, reason)
      delay = retry_after.nil? ? @retry_delay * attempt : retry_after
      @logger&.warn("LessorMQ request failed, retrying (path=#{path} attempt=#{attempt} delay=#{delay} " \
                    "reason=#{reason})")
      sleep(delay)
    end

    def parse(raw)
      return nil if raw.nil? || raw.empty?

      JSON.parse(raw)
    rescue JSON::ParserError
      # A proxy's error page, say: the status says enough.
      nil
    end

    def retry_after(headers)
      value = headers && headers["retry-after"]
      Integer(value, 10) if value
    rescue ArgumentError, TypeError
      nil
    end

    def message_path(lease, action)
      "/v1/messages/#{segment(lease.message.id)}/#{action}"
    end

    # A path segment, percent-encoded (a/b is a%2Fb).
    def segment(value)
      value.to_s.b.gsub(/[^A-Za-z0-9\-._~]/n) { |c| format("%%%02X", c.ord) }
    end

    # Where the exception was raised: the innermost frame of its backtrace.
    def origin(error)
      location = error.backtrace_locations&.first
      return { "file" => location.path, "line" => location.lineno } if location

      line = error.backtrace&.first
      line && (match = line.match(/\A(.+?):(\d+)/)) ? { "file" => match[1], "line" => match[2].to_i } : nil
    end

    def utf8(text)
      text = text.to_s
      return text.scrub if text.encoding == Encoding::UTF_8

      text.encode(Encoding::UTF_8, invalid: :replace, undef: :replace)
    end
  end
end
