# frozen_string_literal: true

# Fails every time, through retry_on: the queue decides about the next attempt.
class FlakyJob < ApplicationJob
  queue_as :flaky
  retry_on IOError, attempts: 10, wait: 1.hour

  cattr_accessor :executions_seen, default: []

  def perform
    executions_seen << executions
    raise IOError, "upstream slow"
  end
end
