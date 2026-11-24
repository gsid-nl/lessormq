# frozen_string_literal: true

module LessorMQ
  module Rails
    # Included in ActiveJob::Base. A job that the LessorMQ worker runs leaves retries to
    # the queue: retry_job (and so retry_on) fails the message instead of enqueueing a
    # copy, which would reset the server's attempt count and backoff.
    module JobExtension
      def self.included(base)
        base.class_attribute :lessormq_publish_options, instance_accessor: false, default: {}.freeze
        base.extend ClassMethods
      end

      module ClassMethods
        # Options for every publish of this job class:
        #
        # max_attempts:: attempts for its messages, within the queue's max_attempts_limit
        # dedup_key::    a String, or a callable that gets the job, for at most one open
        #                message per key
        #
        #   class InvoiceJob < ApplicationJob
        #     lessormq_options max_attempts: 3, dedup_key: ->(job) { "invoice-#{job.arguments.first}" }
        #   end
        def lessormq_options(max_attempts: nil, dedup_key: nil)
          self.lessormq_publish_options = { max_attempts: max_attempts, dedup_key: dedup_key }.compact.freeze
        end
      end

      # The LessorMQ::Job (lease, message, extend_lease, lease_lost?) while the LessorMQ
      # worker runs this job; nil otherwise.
      attr_accessor :lessormq_job

      # In the worker: the error that made the job ask for a retry is raised, so the
      # message is failed and the queue decides about the next attempt. Elsewhere (a job
      # performed inline) ActiveJob enqueues a copy, as always.
      def retry_job(options = {})
        return super if lessormq_job.nil?

        @lessormq_retry_requested = true
        # retry_on passes the error; a job continuation (Rails 8.1) retries from its rescue.
        error = options[:error] || $!
        raise error if error

        raise RetryRequested, "#{self.class.name} asked to be retried"
      end

      # Whether the job asked to be retried while the worker ran it.
      def lessormq_retry_requested?
        @lessormq_retry_requested == true
      end

      def rescue_with_handler(...)
        handled = super
        @lessormq_unhandled = !handled if lessormq_job
        handled
      end

      private

      # ActiveJob runs after_discard for an error that no handler took, since its adapters
      # then drop the job or retry it on their own. Here the queue may try the message
      # again, so after_discard only runs when the job itself gives up: discard_on, or a
      # retry_on whose block ran.
      def run_after_discard_procs(exception)
        return if lessormq_job && @lessormq_unhandled

        super
      end
    end
  end
end
