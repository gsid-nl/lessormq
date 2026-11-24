# frozen_string_literal: true

# LessorMQ for Ruby: the message queue over HTTP where the server decides and clients
# only claim, run, ack or fail.
#
#   client = LessorMQ::Client.new("https://mq.example.com", ENV.fetch("LESSORMQ_KEY"))
#   client.publish("mail", { "to" => "ada@example.com" })
#   LessorMQ::Worker.new(client, "mail") { |message, _job| deliver(message.body) }.run
module LessorMQ
end

require_relative "lessormq/version"
require_relative "lessormq/errors"
require_relative "lessormq/models"
require_relative "lessormq/transport"
require_relative "lessormq/client"
require_relative "lessormq/worker"
