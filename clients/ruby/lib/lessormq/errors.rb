# frozen_string_literal: true

module LessorMQ
  # Everything this library raises.
  class Error < StandardError; end

  # The server could not be reached, after the configured retries.
  class TransportError < Error; end

  # The server answered with an error (RFC 9457 problem details). +code+ is the stable
  # problem code to branch on; the subclasses cover the ones a client acts on.
  class ApiError < Error
    attr_reader :status, :code, :detail, :errors

    def initialize(status, code, detail = "", errors = [])
      @status = status
      @code = code
      @detail = detail
      @errors = errors
      super(detail.empty? ? "LessorMQ error #{status} (#{code})" : detail)
    end

    # The error for an answer, of the subclass for its code.
    def self.from_response(status, body, retry_after = nil)
      problem = body.is_a?(Hash) ? body : {}
      code = problem["code"].is_a?(String) ? problem["code"] : code_for(status)
      detail = problem["detail"].is_a?(String) ? problem["detail"] : ""
      errors = problem["errors"].is_a?(Array) ? problem["errors"] : []

      return RateLimited.new(status, code, detail, errors, retry_after) if code == "rate_limited"

      klass = BY_CODE.fetch(code) { status >= 500 ? ServerError : ApiError }
      klass.new(status, code, detail, errors)
    end

    def self.code_for(status)
      case status
      when 401 then "unauthorized"
      when 403 then "forbidden"
      when 404 then "not_found"
      when 413 then "payload_too_large"
      when 429 then "rate_limited"
      when 500.. then "internal"
      else "validation_failed"
      end
    end
    private_class_method :code_for
  end

  # The receipt is not the current lease of the message. The lease expired, was
  # released, or the message was claimed again. Stop reporting on it: the server has
  # already made it available again.
  class LeaseLost < ApiError; end

  # The request was rejected as invalid; +errors+ names each field.
  class ValidationFailed < ApiError; end

  # The API key is missing, unknown or revoked.
  class Unauthorized < ApiError; end

  # The key lacks the scope or the queue, or its tenant is suspended (code
  # +tenant_suspended+), or a quota that waiting does not lift (+quota_exceeded+).
  class Forbidden < ApiError; end

  # No such queue or message for this tenant.
  class NotFound < ApiError; end

  # The message is in a state that does not allow this (+invalid_state+).
  class Conflict < ApiError; end

  # Body and headers exceed the queue's +max_message_bytes+.
  class PayloadTooLarge < ApiError; end

  # The tenant exceeded a rate limit or its open-message quota, after the configured
  # retries. +retry_after+ is the server's Retry-After in seconds, or nil.
  class RateLimited < ApiError
    attr_reader :retry_after

    def initialize(status, code, detail = "", errors = [], retry_after = nil)
      @retry_after = retry_after
      super(status, code, detail, errors)
    end
  end

  # The server failed (5xx), after the configured retries.
  class ServerError < ApiError; end

  class ApiError
    BY_CODE = {
      "lease_lost" => LeaseLost,
      "validation_failed" => ValidationFailed,
      "unauthorized" => Unauthorized,
      "forbidden" => Forbidden,
      "tenant_suspended" => Forbidden,
      "quota_exceeded" => Forbidden,
      "not_found" => NotFound,
      "invalid_state" => Conflict,
      "payload_too_large" => PayloadTooLarge
    }.freeze
  end

  # Marks an error as permanent: the message itself cannot be processed (it cannot be
  # decoded, it fails validation), so another attempt would only fail again. Include it
  # in your own error class, or raise PermanentFailure. A fact for the server: the
  # queue's +on_permanent+ decides what it means.
  #
  #   class BadInput < ArgumentError
  #     include LessorMQ::Permanent
  #   end
  module Permanent; end

  # Raise from a handler when the message itself cannot be processed.
  class PermanentFailure < StandardError
    include Permanent
  end

  # Whether an error is marked as permanent: it includes Permanent, or it has a
  # +permanent?+ method that says so.
  def self.permanent?(error)
    return true if error.is_a?(Permanent)

    error.respond_to?(:permanent?) && error.permanent? ? true : false
  end
end
