---
title: "LessorMQ for Rails"
description: "An ActiveJob queue adapter and worker for LessorMQ, the message queue over HTTP where the server decides and clients only claim, run, ack or fail."
---

# LessorMQ for Rails

An ActiveJob queue adapter and worker for [LessorMQ](../../README.md), the message
queue over HTTP where the server decides and clients only claim, run, ack or fail. The
queue decides what happens to a failed job (another attempt, when, or the dead-letter
set); Rails only enqueues jobs and runs them.

The gem is `lessormq-rails`, for Rails 7.2, 8.0 and 8.1 on Ruby 3.3 or later. It builds on
the Ruby client, [lessormq](https://github.com/gsid-nl/lessormq/tree/main/clients/ruby), of
the same version (or a later patch).

## Install

```sh
bundle add lessormq-rails
```

It brings the `lessormq` gem with it.

## Configure

```ruby
# config/application.rb (or config/environments/production.rb)
config.active_job.queue_adapter = :lessormq

config.lessormq.url = ENV["LESSORMQ_URL"]     # the default
config.lessormq.api_key = ENV["LESSORMQ_KEY"] # the default
config.lessormq.queues = %w[mail default]     # what the worker takes; ["default"] by default
```

The key needs the `publish` scope for the app and `consume` for the worker (see
[tenants and keys](../protocol.md#tenants-and-keys)). Other settings
under `config.lessormq`:

| Setting | Default | |
|---|---|---|
| `threads` | 1 | jobs run at the same time, per queue |
| `wait_seconds` | 20 | how long a claim waits for a job (at most 20) |
| `lease_seconds` | the queue's | lease per claim, and per keepalive |
| `extend_every` | a third of the lease | seconds between keepalives |
| `consumer` | host:pid | shown to administrators, in the server's list of workers |
| `route` | none | the path this worker runs jobs through (VPN, proxy, account) |
| `http_retries` | 3 | retries after network errors, 5xx and 429 |
| `timeout` | 10 | seconds per request; a claim gets its wait on top |
| `client` | made from `url` and `api_key` | a `LessorMQ::Client` of your own |

There is no setting for retries, delays or dead-lettering: set those on the queue
(`PATCH /v1/queues/{queue}`, see [concepts](../concepts.md#queues-and-their-settings)).
Each thread needs a database connection, so keep the pool at least `threads` times the
number of queues.

## Dispatch

Jobs are ordinary ActiveJob jobs:

```ruby
class WelcomeMailJob < ApplicationJob
  queue_as :mail

  def perform(user)
    UserMailer.welcome(user).deliver_now
  end
end

WelcomeMailJob.perform_later(user)
WelcomeMailJob.set(wait: 10.minutes).perform_later(user)
```

A job is published to the LessorMQ queue named by its `queue_name`, with its serialized
form as the body (readable in the queue) and its class as the `type` header.
`job.provider_job_id` is the message id. A publish the server refuses raises
`ActiveJob::EnqueueError`, so `perform_later` returns false.

Per job class, `lessormq_options` asks for fewer or more attempts than the queue's
default, and keeps at most one open message per key:

```ruby
class InvoiceJob < ApplicationJob
  lessormq_options max_attempts: 3, dedup_key: ->(job) { "invoice-#{job.arguments.first.id}" }
end
```

## Run workers

```sh
bin/rails lessormq:work
bin/rails lessormq:work QUEUES=mail,default THREADS=4
```

The task boots the app (eager loaded when `config.eager_load` is on) and runs a worker
per queue, `THREADS` jobs at a time each, until SIGTERM or SIGINT. Then every worker
stops after its current jobs. A claim that is still waiting is not cut off: it ends
within `wait_seconds`, and a job it brings is given back unrun. So give the process at
least `wait_seconds` plus your longest job to stop (in Kubernetes,
`terminationGracePeriodSeconds`), or lower `wait_seconds`.

Without the task (another process manager, a test), in the booted app:

```ruby
LessorMQ::Rails::Runner.new(queues: %w[mail], threads: 2).run
```

## How it behaves

- **A job that returns**: the message is acked.
- **A job that raises**: the message is failed with the exception's class and message.
  The queue decides whether and when it comes back (its `max_attempts` and `backoff`),
  and whether it goes to the dead-letter set after the last attempt.
- **`retry_on`**: a matching exception fails the message like any other. ActiveJob would
  enqueue a copy instead, which would reset the server's attempt count; under the worker,
  `retry_job` raises the error instead. Its `attempts`, `wait`, `jitter`, `queue` and
  `priority` do not apply: the queue's settings do. (Its counter only sees the current
  delivery, so only `attempts: 1` makes it give up: then its block runs and the message
  fails as permanent.) A `retry_job` of your own (from a `rescue_from`) fails the message
  the same way.
- **`discard_on`**: the job gives up, so the message is failed as permanent with the
  original error. The queue's `on_permanent` decides: the dead-letter set at once by
  default, or another attempt with `retry`. Its block runs as usual.
- **Permanent failures**: an error that includes `LessorMQ::Permanent` (or raise
  `LessorMQ::PermanentFailure`), and `ActiveJob::DeserializationError` (a record that is
  gone, an argument that cannot be read), are reported as permanent. A `retry_on
  ActiveJob::DeserializationError` makes the latter an ordinary failure again. A message
  that is not an ActiveJob job is permanent too; a job class this worker does not know
  is not (during a deploy, the next attempt may reach a worker that has it).
- **`executions`** is the server's attempt number. It is for logging; nothing in this
  gem decides on it.
- **`after_discard`** runs when the job gives up (`discard_on`). Not after an exception
  that nothing handled: the queue may try that message again. A message the server gives
  up on is in the dead-letter set.
- **Failed jobs**: there is no failed-jobs table. Messages that ran out of attempts, or
  failed permanently, stay in the queue's dead-letter set for its retention, where an
  administrator can inspect, replay or delete them.
- **Release**: a worker that is stopped while a claim waits gives the job back with a
  release: the attempt does not count.
- **Timeouts**: ActiveJob has none, and neither does the worker. While `perform` runs,
  a thread extends the lease every third of it. If the lease is lost anyway, the outcome
  is not reported (the message is someone else's by then); a long job can check
  `lessormq_job&.lease_lost?` and stop. A worker that dies mid-job leaves the lease to
  expire, which counts as a failed attempt, so a job that crashes its worker still runs
  out of attempts.
- **Scheduling**: `wait` and `wait_until` become the message's delay, in whole seconds
  from the server's clock, rounded up.
- **Priority** does not exist in LessorMQ and is not sent; use separate queues.
- **Errors** reach Rails' error reporter (and so Sentry, Honeybadger, ...) as in any
  ActiveJob backend: every failed attempt; a discarded job only with `report: true`
  (Rails 8.1).
- **Job continuations** (Rails 8.1) run their steps, but the progress is not kept between
  attempts: LessorMQ redelivers the message as it was published, so a failed attempt
  starts again from the first step. The worker does not interrupt jobs when it stops;
  it waits for them.
- **Enqueue after commit**: a job enqueued inside a database transaction is published
  after the commit when `enqueue_after_transaction_commit` is on: by default on Rails
  7.2, with `config.active_job.enqueue_after_transaction_commit = true` on 8.x.

## Moving from another queue

- Let the queue decide about retries: set the queue's `max_attempts` and `backoff` to what
  your `retry_on` (or Sidekiq's `retry`) did. `retry_on` can stay, it is harmless, but it
  no longer chooses the wait or the number of attempts.
- A failed-jobs UI is not needed: messages that ran out of attempts stay in the queue's
  dead-letter set, where an administrator can look at and replay them.
- Uniqueness is `lessormq_options dedup_key:`: at most one open message per key.
- Workers on different network paths (VPN exits, proxies) set `config.lessormq.route`;
  see [routes](../concepts.md#routes).
- Move one queue at a time: give the job classes of that queue
  `self.queue_adapter = :lessormq`, run `bin/rails lessormq:work QUEUES=that-queue` beside
  your old workers, and switch `config.active_job.queue_adapter` when the last one has
  moved.

## Development

```sh
cd clients/rails
bundle install
bundle exec rake test
```

The unit tests use ActiveJob's test helpers and a scripted LessorMQ server, no server
needed. `RAILS_VERSION=7.2` (or `8.0`, `8.1`, the default) picks the Rails to test
against. The end-to-end tests boot a small Rails app (`test/dummy`), dispatch jobs
through ActiveJob and run them with the worker and with `rake lessormq:work`, against a
running server (with `LEASE_SWEEP_SECONDS=1`) and its operator key:

```sh
LESSORMQ_URL=http://127.0.0.1:8080 LESSORMQ_OPERATOR_KEY=lmq_... bundle exec rake e2e
```

They create a tenant of their own and delete it afterwards. The Ruby client's own tests
and conformance adapter are in
[clients/ruby](https://github.com/gsid-nl/lessormq/tree/main/clients/ruby).

_From [clients/rails/README.md](https://github.com/gsid-nl/lessormq/blob/main/clients/rails/README.md)._
