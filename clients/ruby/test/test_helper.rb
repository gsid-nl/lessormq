# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "json"
require "minitest/autorun"
require "lessormq"
require_relative "fake_transport"

LEASE = {
  "receipt" => "r1",
  "lease_until" => "2030-01-01T00:00:30Z",
  "message" => {
    "id" => "m1",
    "queue" => "mail",
    "body" => { "to" => "ada@example.com" },
    "headers" => { "type" => "Welcome" },
    "attempt" => 1,
    "max_attempts" => 7,
    "created_at" => "2030-01-01T00:00:00Z"
  }
}.freeze

FAIL_RESULT = { "outcome" => "retrying", "available_at" => "2030-01-01T00:00:10Z", "attempt" => 1,
                "max_attempts" => 7 }.freeze

PUBLISHED = {
  "id" => "m1", "queue" => "mail", "status" => "pending", "created_at" => "2030-01-01T00:00:00Z",
  "available_at" => "2030-01-01T00:00:00Z", "duplicate" => false
}.freeze
