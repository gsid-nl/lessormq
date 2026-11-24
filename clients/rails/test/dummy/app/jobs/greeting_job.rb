# frozen_string_literal: true

# Succeeds.
class GreetingJob < ApplicationJob
  def perform(name)
    Rails.logger.info("Hello #{name}, attempt #{executions}")
  end
end
