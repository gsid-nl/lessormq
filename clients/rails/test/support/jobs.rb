# frozen_string_literal: true

# Jobs for the unit tests. Each records what it saw in Runs.
module Runs
  @lock = Mutex.new
  @items = []

  def self.add(item)
    @lock.synchronize { @items << item }
  end

  def self.all
    @lock.synchronize { @items.dup }
  end

  def self.clear
    @lock.synchronize { @items.clear }
  end
end

class ApplicationJob < ActiveJob::Base
end

class RecordingJob < ApplicationJob
  queue_as :mail

  def perform(*args)
    Runs.add(job: :recording, args: args, executions: executions, provider_job_id: provider_job_id,
             lease: lessormq_job&.lease&.receipt)
  end
end

class FailingJob < ApplicationJob
  def perform
    Runs.add(job: :failing, executions: executions)
    raise IOError, "upstream slow"
  end
end

class RetryOnJob < ApplicationJob
  retry_on IOError, attempts: 3, wait: 10

  def perform
    Runs.add(job: :retry_on, executions: executions)
    raise IOError, "upstream slow"
  end
end

class DiscardOnJob < ApplicationJob
  discard_on(ArgumentError) { |_job, error| Runs.add(job: :discard_block, error: error.message) }
  after_discard { |_job, error| Runs.add(job: :after_discard, error: error.message) }

  def perform(fail_with)
    raise ArgumentError, "bad input" if fail_with == "argument"
    raise IOError, "upstream slow" if fail_with == "io"
  end
end

class RetryJobJob < ApplicationJob
  rescue_from(IOError) { retry_job(wait: 5) }

  def perform
    raise IOError, "try later"
  end
end

class ManualRetryJob < ApplicationJob
  def perform
    retry_job
  end
end

class RescheduleJob < ApplicationJob
  def perform
    enqueue(wait: 60)
  end
end

class PermanentJob < ApplicationJob
  class Unusable < StandardError
    include LessorMQ::Permanent
  end

  def perform
    raise Unusable, "cannot be decoded"
  end
end

class ArgumentJob < ApplicationJob
  def perform(record)
    Runs.add(job: :argument, record: record)
  end
end

class RetryDeserializationJob < ApplicationJob
  retry_on ActiveJob::DeserializationError

  def perform(record)
    Runs.add(job: :argument, record: record)
  end
end

class LimitedJob < ApplicationJob
  lessormq_options max_attempts: 3, dedup_key: ->(job) { "limited-#{job.arguments.first}" }

  def perform(_id); end
end

class GiveUpJob < ApplicationJob
  retry_on(IOError, attempts: 1) { |_job, error| Runs.add(job: :give_up_block, error: error.message) }

  def perform
    raise IOError, "upstream gone"
  end
end
