# frozen_string_literal: true

require "lessormq"
require "active_job"
require_relative "rails/version"

module LessorMQ
  # ActiveJob on LessorMQ. The queue decides what happens to a failed job (another
  # attempt, when, or the dead-letter set); Rails only enqueues and runs jobs.
  #
  # Inside this module +Rails+ is LessorMQ::Rails: the framework is always ::Rails.
  module Rails
    # How to reach the server and how the worker claims. There is nothing about retries:
    # those are the queue's settings.
    class Configuration
      # The server, and a key with the publish and/or consume scope.
      attr_accessor :url, :api_key
      # Queues the worker takes jobs from, and the jobs it runs at the same time per queue.
      attr_accessor :queues, :threads
      # How the worker claims; see LessorMQ::Worker.
      attr_accessor :wait_seconds, :lease_seconds, :extend_every, :consumer, :route
      # The client's HTTP retries and timeout per request.
      attr_accessor :http_retries, :timeout
      # A Logger; Rails.logger once the app is booted.
      attr_accessor :logger
      # A LessorMQ::Client of your own, instead of one made from url and api_key.
      attr_writer :client

      def initialize
        @url = ENV.fetch("LESSORMQ_URL", nil)
        @api_key = ENV.fetch("LESSORMQ_KEY", nil)
        @queues = ["default"]
        @threads = 1
        @wait_seconds = 20
        @http_retries = 3
        @timeout = 10
        @lock = Mutex.new
      end

      # The client every enqueue and the worker use, made on first use.
      def client
        @lock.synchronize do
          @client ||= begin
            raise ArgumentError, "LessorMQ: set config.lessormq.url and api_key (or LESSORMQ_URL and LESSORMQ_KEY)" \
              if url.to_s.empty? || api_key.to_s.empty?

            LessorMQ::Client.new(url, api_key, http_retries: http_retries, timeout: timeout, logger: logger)
          end
        end
      end

      # Never the key.
      def inspect
        "#<LessorMQ::Rails::Configuration url=#{url.inspect} queues=#{queues.inspect}>"
      end
    end

    # The message is not an ActiveJob job: another attempt would not change that.
    class InvalidJob < StandardError
      include LessorMQ::Permanent
    end

    # A job called retry_job without an error of its own.
    class RetryRequested < StandardError; end

    def self.config
      @config ||= Configuration.new
    end

    def self.configure
      yield config
    end
  end
end

require_relative "rails/job_extension"
ActiveSupport.on_load(:active_job) { include LessorMQ::Rails::JobExtension }
require_relative "rails/executor"
require_relative "rails/runner"
require_relative "../active_job/queue_adapters/lessormq_adapter"
require_relative "rails/railtie" if defined?(::Rails::Railtie)
