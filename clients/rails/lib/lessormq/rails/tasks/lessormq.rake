# frozen_string_literal: true

namespace :lessormq do
  desc "Run ActiveJob jobs from LessorMQ until SIGTERM or SIGINT (QUEUES=mail,default THREADS=1)"
  task work: :environment do
    # Rake does not eager load; a worker should, as the app server does.
    Rails.application.eager_load! if Rails.application.config.eager_load
    LessorMQ::Rails::Runner.from_env.run
  end
end
