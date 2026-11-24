# frozen_string_literal: true

require_relative "lib/lessormq/version"

Gem::Specification.new do |spec|
  spec.name = "lessormq"
  spec.version = LessorMQ::VERSION
  spec.authors = ["LessorMQ"]
  spec.summary = "Ruby client and worker for LessorMQ, the message queue over HTTP where the server decides " \
                 "and clients only claim, run, ack or fail"
  spec.homepage = "https://lessormq.dev"
  spec.license = "Apache-2.0"
  spec.required_ruby_version = ">= 3.3"

  spec.metadata = {
    "homepage_uri" => "https://lessormq.dev",
    "documentation_uri" => "https://lessormq.dev/docs/examples",
    "source_code_uri" => "https://github.com/gsid-nl/lessormq/tree/main/clients/ruby",
    "bug_tracker_uri" => "https://github.com/gsid-nl/lessormq/issues",
    "rubygems_mfa_required" => "true"
  }

  spec.files = Dir["lib/**/*.rb"] + %w[README.md LICENSE]
  spec.require_paths = ["lib"]
  # Standard library only: net/http, json, openssl.
end
