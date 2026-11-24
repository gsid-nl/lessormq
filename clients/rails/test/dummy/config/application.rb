# frozen_string_literal: true

# A minimal Rails app for the end-to-end tests: ActiveJob on LessorMQ, nothing else.
require "logger"
require "rails"
require "active_job/railtie"
require "lessormq/rails"

module Dummy
  class Application < Rails::Application
    config.load_defaults Rails::VERSION::STRING.to_f
    config.root = File.expand_path("..", __dir__)
    config.eager_load = false
    config.logger = ActiveSupport::Logger.new(ENV.fetch("DUMMY_LOG", File::NULL))
    config.active_job.queue_adapter = :lessormq
    # LESSORMQ_URL and LESSORMQ_KEY come from the environment.
    config.lessormq.wait_seconds = 2
  end
end
