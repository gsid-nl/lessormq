# frozen_string_literal: true

require_relative "lib/lessormq/rails/version"

Gem::Specification.new do |spec|
  spec.name = "lessormq-rails"
  spec.version = LessorMQ::Rails::VERSION
  spec.authors = ["LessorMQ"]
  spec.summary = "ActiveJob adapter and worker for LessorMQ: the queue decides retries, delays and " \
                 "dead-lettering; Rails only enqueues and runs jobs"
  spec.homepage = "https://lessormq.dev"
  spec.license = "Apache-2.0"
  spec.required_ruby_version = ">= 3.3"

  spec.metadata = {
    "homepage_uri" => "https://lessormq.dev",
    "documentation_uri" => "https://lessormq.dev/docs/examples",
    "source_code_uri" => "https://github.com/gsid-nl/lessormq/tree/main/clients/rails",
    "bug_tracker_uri" => "https://github.com/gsid-nl/lessormq/issues",
    "rubygems_mfa_required" => "true"
  }

  spec.files = Dir["lib/**/*.{rb,rake}"] + %w[README.md LICENSE]
  spec.require_paths = ["lib"]

  spec.add_dependency "activejob", ">= 7.2", "< 9"
  # Its own version or a later patch, as lessormq/messenger requires lessormq/client.
  spec.add_dependency "lessormq", "~> #{LessorMQ::Rails::VERSION}"
  spec.add_dependency "railties", ">= 7.2", "< 9"
end
