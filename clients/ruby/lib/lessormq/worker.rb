# frozen_string_literal: true

require "socket"

module LessorMQ
  # What a handler gets besides the message: the lease, and a way to keep it.
  #
  # The worker already extends the lease on a timer; call #extend_lease yourself only
  # with +auto_extend: false+, or to ask for a longer lease at once.
  class Job
    attr_reader :lease

    def initialize(client, lease)
      @client = client
      @lease = lease
      @lease_lost = false
    end

    def message
      @lease.message
    end

    # Raises LeaseLost when the lease already ended.
    def extend_lease(lease_seconds: nil)
      @client.extend_lease(@lease, lease_seconds: lease_seconds)
    rescue LeaseLost
      @lease_lost = true
      raise
    end

    # Whether the lease was lost while the handler ran: the message is someone else's
    # now, and the outcome will not be reported. A long handler can check it and stop.
    def lease_lost?
      @lease_lost
    end
  end

  # Claims messages of one queue and runs a handler on each:
  #
  #   worker = LessorMQ::Worker.new(client, "mail") do |message, job|
  #     Mailer.deliver(message.body)
  #   end
  #   worker.run
  #
  # The handler returns: the message is acked. It raises: the message is failed with the
  # exception's class and message, as permanent when LessorMQ.permanent?(error). What
  # happens next (another attempt, when, or the dead-letter set) is decided by the queue.
  # While the handler runs, a thread extends the lease every third of it. On SIGTERM or
  # SIGINT the worker stops after the current messages.
  class Worker
    # What a handler may raise to fail its message. Signals, exit and running out of
    # memory end the worker instead; the lease then expires and the attempt counts.
    HANDLER_ERRORS = [StandardError, ScriptError, SystemStackError].freeze

    STOP_SIGNALS = %w[TERM INT].freeze

    attr_reader :queue

    # consumer::       shown to administrators; host:pid by default
    # route::          the path this worker runs jobs through (a VPN exit, a proxy)
    # wait_seconds::   long-poll wait per claim (at most 20)
    # lease_seconds::  lease per claim and per extend; the queue's when nil
    # extend_every::   seconds between automatic extends; a third of the lease when nil
    # auto_extend::    extend the lease while a handler runs
    # concurrency::    messages handled at the same time, each in a thread of its own
    #                  with its own claims
    # handle_signals:: stop after the current messages on SIGTERM or SIGINT
    # max_messages::   stop after this many messages
    # max_seconds::    stop claiming after running this long
    # error_pause::    seconds to wait after the server could not be reached
    # logger::         a Logger; the client's by default
    def initialize(client, queue, handler = nil, consumer: nil, route: nil, wait_seconds: 20, lease_seconds: nil,
                   extend_every: nil, auto_extend: true, concurrency: 1, handle_signals: true, max_messages: nil,
                   max_seconds: nil, error_pause: 1.0, logger: nil, &block)
      @handler = handler || block
      raise ArgumentError, "a handler is required: a block, or anything that responds to call" unless @handler

      @client = client
      @queue = queue
      @consumer = consumer || Worker.default_consumer
      @route = route
      @wait_seconds = wait_seconds
      @lease_seconds = lease_seconds
      @extend_every = extend_every
      @auto_extend = auto_extend
      @concurrency = [concurrency.to_i, 1].max
      @handle_signals = handle_signals
      @max_messages = max_messages
      @max_seconds = max_seconds
      @error_pause = error_pause
      @logger = logger || client.logger
      @counter = Mutex.new
      @started = 0
      @handled = 0
      @stopping = false
      @wake = Thread::Queue.new
    end

    # Messages handled so far.
    def handled
      @counter.synchronize { @handled }
    end

    # Claims and handles messages until #stop, a signal, or a configured limit, and
    # returns once every message in hand is reported, with the number handled.
    #
    # A claim already waiting is not cut off when the worker is asked to stop (that
    # could leave a leased message to run out): it ends within +wait_seconds+, and a
    # message it brings is given back with a release. A stopped worker stays stopped,
    # also when #stop came before #run.
    def run
      @deadline = @max_seconds && (monotonic + @max_seconds)
      previous = trap_signals if @handle_signals
      begin
        if @concurrency == 1
          loop_once until stop?
        else
          threads = Array.new(@concurrency) do |i|
            Thread.new { loop_once until stop? }.tap { |thread| thread.name = "lessormq-#{i}" }
          end
          threads.each(&:join)
        end
      ensure
        previous&.each { |signal, handler| Signal.trap(signal, handler) }
      end
      handled
    end

    # One claim, and the handler on what it got. Returns whether a message was handled.
    def run_once
      loop_once
    end

    # Stops after the current messages. Safe from any thread and from a signal handler.
    def stop
      @stopping = true
      @wake.close
    end

    def stopping?
      @stopping
    end

    # The host and the process, so the workers on one host are told apart.
    def self.default_consumer
      host = Socket.gethostname
      host.nil? || host.empty? ? nil : "#{host}:#{Process.pid}"
    rescue SystemCallError, SocketError
      nil
    end

    private

    def stop?
      return true if @stopping
      return true if @deadline && monotonic >= @deadline

      @max_messages ? @counter.synchronize { @started >= @max_messages } : false
    end

    def loop_once
      begin
        lease = @client.claim(
          @queue, consumer: @consumer, route: @route, wait_seconds: @wait_seconds, lease_seconds: @lease_seconds
        )
      rescue Error => e
        @logger&.error("LessorMQ: claim failed (queue=#{@queue} error=#{e.message})")
        @wake.pop(timeout: @error_pause)
        return false
      end
      return false if lease.nil?

      unless !@stopping && take
        # Asked to stop while the claim waited, or the other threads reached
        # max_messages: give the message back untouched.
        report(lease, "release") { @client.release(lease) }
        return false
      end

      handle(lease)
      @counter.synchronize { @handled += 1 }
      true
    end

    # Reserves one of max_messages for a claimed message.
    def take
      return true unless @max_messages

      @counter.synchronize do
        return false if @started >= @max_messages

        @started += 1
      end
      true
    end

    def handle(lease)
      job = Job.new(@client, lease)
      keeper = @auto_extend ? keep(job) : nil
      error = nil
      begin
        @handler.call(lease.message, job)
      rescue *HANDLER_ERRORS => e
        error = e
      ensure
        keeper&.stop
      end

      if job.lease_lost?
        @logger&.warn("LessorMQ: lease lost while the handler ran; not reporting (message_id=#{lease.message.id})")
      elsif error.nil?
        report(lease, "ack") { @client.ack(lease) }
      else
        report(lease, "fail") do
          result = @client.fail(lease, error)
          @logger&.info("LessorMQ: message failed (message_id=#{lease.message.id} error=#{error.message} " \
                        "outcome=#{result.outcome} attempt=#{result.attempt} max_attempts=#{result.max_attempts})")
        end
      end
    end

    # Extends the lease every interval until stopped; a lost lease marks the job.
    def keep(job)
      Keeper.new(job, @lease_seconds, @extend_every, @logger)
    end

    # Ack, fail or release, keeping the worker alive whatever happens: a lost lease means
    # the message is someone else's now; any other failure means the lease will expire
    # and the server will make the message available again.
    def report(lease, what)
      yield
    rescue LeaseLost
      @logger&.warn("LessorMQ: lease lost before #{what}; the message is no longer ours " \
                    "(message_id=#{lease.message.id})")
    rescue Error => e
      @logger&.error("LessorMQ: #{what} failed; the lease will expire and the message come back " \
                     "(message_id=#{lease.message.id} error=#{e.message})")
    end

    def trap_signals
      STOP_SIGNALS.to_h { |signal| [signal, Signal.trap(signal) { stop }] }
    end

    def monotonic
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    # The keepalive thread of one job.
    class Keeper
      def initialize(job, lease_seconds, extend_every, logger)
        @job = job
        @lease_seconds = lease_seconds
        @extend_every = extend_every
        @logger = logger
        @done = Thread::Queue.new
        @thread = Thread.new { run }
        @thread.name = "lessormq-extend"
      end

      def stop
        @done.close
        @thread.join
      end

      private

      def run
        # pop returns nil on the timeout, and at once once the queue is closed.
        until @done.pop(timeout: interval) || @done.closed?
          begin
            @job.extend_lease(lease_seconds: @lease_seconds)
          rescue LeaseLost
            return
          rescue Error => e
            @logger&.warn("LessorMQ: extend failed; trying again (error=#{e.message})")
          end
        end
      end

      def interval
        @extend_every || [1.0, @job.lease.seconds_left / 3].max
      end
    end
    private_constant :Keeper
  end
end
