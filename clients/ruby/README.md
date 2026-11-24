# LessorMQ for Ruby

Client and worker for [LessorMQ](https://lessormq.dev), the message queue over HTTP where
the server decides and clients only claim, run, ack or fail. Retries, backoff, attempts
and the dead-letter set are the queue's settings; this library reports what happened and
leaves the rest to the server.

The gem is `lessormq`. It needs Ruby 3.3 or later and uses the standard library only
(`net/http`, `json`, `openssl`). For ActiveJob, see
[lessormq-rails](https://github.com/gsid-nl/lessormq/tree/main/clients/rails).

## Install

```sh
gem install lessormq
```

or in a Gemfile:

```ruby
gem "lessormq"
```

## Connect

```ruby
require "lessormq"

client = LessorMQ::Client.new("https://mq.example.com", ENV.fetch("LESSORMQ_KEY"))
```

The key needs the `publish` scope to publish and `consume` to work a queue (see
[tenants and keys](https://lessormq.dev/docs/protocol#tenants-and-keys)). Options:
`http_retries` (3), `retry_delay` (0.1 seconds, times the attempt), `timeout` (10
seconds per request; a claim gets its wait on top), `auto_dedup_key` (true),
`logger` (a `Logger` for the retries; nothing is logged by default), and `transport`
(anything that answers `request(method, path, headers, body, timeout)`, for a proxy or
a test). The client is thread-safe and keeps its connections alive.

## Publish

```ruby
client.publish("mail", { to: "ada@example.com", template: "welcome" })

client.publish(
  "invoices",
  { invoice: 1042 },
  dedup_key: "invoice-1042",  # one open message per key
  delay_seconds: 600,          # or available_at: Time
  max_attempts: 3,             # within the queue's max_attempts_limit
  headers: { "type" => "InvoiceDue" }
)
```

The body is any JSON value. Every publish carries a dedup key (your own, or one the client
makes up), so the client can safely retry it after a network error without creating a
second message. `publish` returns a `LessorMQ::Published` (`id`, `status`,
`available_at`, `duplicate?`).

## Work

```ruby
worker = LessorMQ::Worker.new(client, "mail", route: ENV["VPN_ID"], concurrency: 4) do |message, job|
  raise LessorMQ::PermanentFailure, "no recipient" unless message.body["to"]

  Mailer.deliver(message.body)
end

worker.run
```

- The block returns: the message is acked.
- It raises: the message is failed with the exception's class and message. Raise
  `LessorMQ::PermanentFailure`, or include `LessorMQ::Permanent` in your own error class
  (or give it a `permanent?` method), when the input itself is unusable.
- What happens after a failure is not up to the worker. The queue's settings decide
  whether and when the message comes back, and when it goes to the dead-letter set.
- While the block runs, a thread extends the lease every third of it. A block that
  outlives its lease anyway is not reported: the message is someone else's by then.
  `job.lease_lost?` tells a long job to stop early.
- `concurrency` runs that many messages side by side, each in a thread with its own
  claims.
- SIGTERM and SIGINT stop the worker after the current messages. A claim that is already
  waiting is not cut off: it ends within `wait_seconds`, and a message it brings is given
  back unrun.

The handler can also be anything that responds to `call(message, job)`. `message` has
`id`, `queue`, `body`, `headers`, `attempt`, `max_attempts`, `created_at` and
`last_attempt?`; `job` has `lease`, `message`, `extend_lease` and `lease_lost?`.

`Worker` options: `consumer` (host:pid by default), `route`, `wait_seconds` (20),
`lease_seconds`, `extend_every`, `auto_extend`, `concurrency` (1), `handle_signals`,
`max_messages`, `max_seconds`, `error_pause` (1 second), `logger`. `worker.stop` stops
it from another thread.

## Without the worker

```ruby
lease = client.claim("mail", wait_seconds: 20)
if lease
  begin
    Mailer.deliver(lease.message.body)
    client.ack(lease)
  rescue StandardError => e
    result = client.fail(lease, e) # result.outcome: retrying, dead_lettered or discarded
  end
end
```

`client.extend_lease(lease)` keeps a lease while a long job runs (it is not called
`extend`, which every Ruby object already has), and `client.release(lease)` hands a
message back without counting the attempt. All of them raise `LessorMQ::LeaseLost` once
the lease has ended: drop the message then, it is someone else's now.

`client.fail(lease, error, permanent: true)` reports a permanent failure; `details:` and
`type:` override what is taken from the exception, and a String works as the error too.
The answer says what the server did. It is for logging: the queue has decided already.

Many short jobs go faster in batches: `client.claim_batch("mail", 50)` leases up to 50
ready messages at once (each with its own lease), and `client.ack_batch(leases)` acks them
in one request, with an outcome per lease (`acked`, `lease_lost`, ...). A message that
fails is reported on its own with `fail`. `client.stats("mail")` reads a queue's counts.

## Errors

Everything the library raises is a `LessorMQ::Error`: `LessorMQ::TransportError` when the
server cannot be reached, `LessorMQ::ApiError` (with `status`, `code`, `detail` and
`errors`) when it answers with a
[problem](https://lessormq.dev/docs/protocol#errors), in subclasses for the codes a
client acts on: `LeaseLost`, `ValidationFailed`, `Unauthorized`, `Forbidden` (also
`tenant_suspended` and `quota_exceeded`), `NotFound`, `Conflict`, `PayloadTooLarge`,
`RateLimited` (with `retry_after`) and `ServerError`.

Network errors, 5xx and 429 are retried (`http_retries: 3`, `retry_delay: 0.1` seconds
times the attempt, or the server's `Retry-After`), for every call that is safe to repeat:
all of them, given the automatic dedup key on publish. That is the only retrying the
library does.

## Development

```sh
cd clients/ruby
bundle install
bundle exec rake test
```

The unit tests run against a scripted transport (`test/fake_transport.rb`), no server
needed. The library passes the
[conformance suite](https://github.com/gsid-nl/lessormq/tree/main/conformance) through
`test/conformance_adapter.rb`, from the repository root:

```sh
LESSORMQ_ADAPTER_CMD="ruby clients/ruby/test/conformance_adapter.rb" conformance/run.sh
```

See [writing a client](https://lessormq.dev/docs/writing-a-client) for the rules it follows.
