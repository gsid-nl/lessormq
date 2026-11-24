# frozen_string_literal: true

# Gives up at once, through discard_on: a permanent failure.
class UnusableJob < ApplicationJob
  queue_as :unusable
  discard_on ArgumentError

  def perform
    raise ArgumentError, "cannot be decoded"
  end
end
