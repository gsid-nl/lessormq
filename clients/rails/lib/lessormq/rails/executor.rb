# frozen_string_literal: true

module LessorMQ
  module Rails
    # Runs the ActiveJob job in a message: the handler of the worker's LessorMQ::Worker.
    #
    # It returns: the message is acked. It raises: the message is failed with the job's
    # error, and the queue decides what follows. The error is marked permanent when the
    # job gave up on it (discard_on, or a retry_on whose block ran), when the job's
    # arguments cannot be deserialized, or when it is a LessorMQ::Permanent error.
    class Executor
      GIVE_UP_EVENTS = /\A(discard|retry_stopped)\.active_job\z/

      # The error itself, marked permanent, so the server gets its own class and message.
      def self.permanent(error)
        error.extend(LessorMQ::Permanent) unless error.frozen? || LessorMQ.permanent?(error)
        error
      end

      def initialize
        # ActiveJob::Base brings ActiveJob::Callbacks, and runs the app's on_load hooks.
        # Not before: an executor is made when the worker starts, after the app has booted.
        require "active_job/base"
      end

      def call(message, lease_job)
        data = message.body
        unless data.is_a?(Hash) && data["job_class"].is_a?(String)
          raise InvalidJob, "message #{message.id} is not an ActiveJob job (no job_class)"
        end

        given_up = nil
        ActiveJob::Callbacks.run_callbacks(:execute) { given_up = perform(data, message, lease_job) }
        # Raised outside the app's executor: a job that gave up is not an unhandled error
        # for Rails' error reporter, as in ActiveJob (discard_on report: true, Rails 8.1,
        # reports it itself).
        raise Executor.permanent(given_up) if given_up
      end

      private

      # Returns the error the job gave up on, if it did.
      def perform(data, message, lease_job)
        # An unknown class is a NameError, not permanent: during a rolling deploy the
        # next attempt may well reach a worker that has it.
        job = ActiveJob::Base.deserialize(data)
        # The server counts the attempts: perform_now adds this one.
        job.executions = message.attempt - 1
        job.provider_job_id = message.id
        job.lessormq_job = lease_job

        given_up = nil
        watch = lambda do |_name, _started, _finished, _id, payload|
          given_up ||= payload[:error] if payload[:job].equal?(job)
        end
        begin
          ActiveSupport::Notifications.subscribed(watch, GIVE_UP_EVENTS) { job.perform_now }
        rescue ActiveJob::DeserializationError => e
          # A record that no longer exists, an argument that cannot be read: another
          # attempt would fail the same way, unless the job asked for one (retry_on).
          raise if job.lessormq_retry_requested?

          raise Executor.permanent(e)
        end
        given_up
      end
    end
  end
end
