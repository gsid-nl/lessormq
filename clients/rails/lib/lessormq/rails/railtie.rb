# frozen_string_literal: true

module LessorMQ
  module Rails
    # config.lessormq in the app, the Rails logger, and the lessormq:work task.
    class Railtie < ::Rails::Railtie
      config.lessormq = LessorMQ::Rails.config

      initializer "lessormq.logger" do
        LessorMQ::Rails.config.logger ||= ::Rails.logger
      end

      rake_tasks do
        load File.expand_path("tasks/lessormq.rake", __dir__)
      end
    end
  end
end
