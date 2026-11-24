# frozen_string_literal: true

# Loaded by lessormq/rails, which brings the client and the configuration.

module ActiveJob
  module QueueAdapters
    # The LessorMQ queue adapter:
    #
    #   config.active_job.queue_adapter = :lessormq
    #
    # A job is published to the LessorMQ queue of its queue_name, its serialized form as
    # the body and its class as the +type+ header. A scheduled job gets a delay; the
    # server makes it available then. Priorities do not exist in LessorMQ and are not sent.
    #
    # The jobs are run by the worker: bin/rails lessormq:work.
    class LessormqAdapter < AbstractAdapter
      # +client+: a LessorMQ::Client; by default the one of config.lessormq.
      def initialize(client = nil)
        super()
        @client = client
      end

      def enqueue(job)
        publish(job, nil)
      end

      def enqueue_at(job, timestamp)
        publish(job, timestamp)
      end

      private

      def publish(job, timestamp)
        options = job.class.respond_to?(:lessormq_publish_options) ? job.class.lessormq_publish_options : {}
        dedup_key = options[:dedup_key]
        dedup_key = dedup_key.call(job) if dedup_key.respond_to?(:call)
        # Whole seconds from the server's clock, rounded up so a job never runs early.
        delay = timestamp ? (timestamp.to_f - Time.now.to_f).ceil : 0
        published = client.publish(
          job.queue_name, job.serialize,
          headers: { "type" => job.class.name.to_s },
          dedup_key: dedup_key&.to_s,
          delay_seconds: delay.positive? ? delay : nil,
          max_attempts: options[:max_attempts]
        )
        job.provider_job_id = published.id
        published
      rescue LessorMQ::Error => e
        raise ActiveJob::EnqueueError, "LessorMQ: #{e.message}"
      end

      def client
        @client || LessorMQ::Rails.config.client
      end
    end
  end
end
