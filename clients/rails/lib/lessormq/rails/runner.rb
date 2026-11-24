# frozen_string_literal: true

module LessorMQ
  module Rails
    # The worker command: a LessorMQ::Worker per queue, each with +threads+ jobs at a
    # time, until SIGTERM or SIGINT. Then every worker stops after its current jobs; a
    # claim that is still waiting ends within wait_seconds, and a job it brings is given
    # back unrun.
    #
    #   bin/rails lessormq:work QUEUES=mail,default THREADS=4
    class Runner
      STOP_SIGNALS = %w[TERM INT].freeze

      attr_reader :workers

      # queues::       the queues to take jobs from
      # threads::      jobs run at the same time, per queue
      # max_messages:: stop each queue's worker after this many jobs (for tests)
      # The rest are LessorMQ::Worker's options; the configuration fills in what is not given.
      def initialize(queues: nil, threads: nil, client: nil, config: LessorMQ::Rails.config, max_messages: nil,
                     **worker_options)
        @config = config
        @queues = Array(queues || config.queues).map(&:to_s).reject(&:empty?)
        raise ArgumentError, "LessorMQ: no queues to work" if @queues.empty?

        @threads = Integer(threads || config.threads)
        @client = client || config.client
        @logger = worker_options[:logger] || config.logger
        options = {
          consumer: config.consumer, route: config.route, wait_seconds: config.wait_seconds,
          lease_seconds: config.lease_seconds, extend_every: config.extend_every, logger: @logger
        }.compact.merge(worker_options.compact)
        executor = Executor.new
        @workers = @queues.map do |queue|
          LessorMQ::Worker.new(@client, queue, executor, concurrency: @threads, handle_signals: false,
                                                         max_messages: max_messages, **options)
        end
      end

      # The runner as the rake task starts it: QUEUES (comma-separated) and THREADS from
      # the environment, over the configuration.
      def self.from_env(env = ENV, config: LessorMQ::Rails.config)
        queues = env["QUEUES"]&.split(",")&.map(&:strip)
        new(queues: queues, threads: env["THREADS"], config: config)
      end

      # Runs until stopped, and returns the number of jobs handled.
      def run(handle_signals: true)
        previous = handle_signals ? STOP_SIGNALS.to_h { |signal| [signal, Signal.trap(signal) { stop }] } : nil
        @logger&.info("LessorMQ: working queues #{@queues.join(', ')} with #{@threads} thread(s) each")
        threads = @workers.map { |worker| Thread.new { worker.run }.tap { |t| t.name = "lessormq-#{worker.queue}" } }
        handled = threads.sum(&:value)
        @logger&.info("LessorMQ: stopped after #{handled} job(s)")
        handled
      ensure
        previous&.each { |signal, handler| Signal.trap(signal, handler) }
      end

      # Stops after the current jobs. Safe from any thread and from a signal handler.
      def stop
        @workers.each(&:stop)
      end
    end
  end
end
