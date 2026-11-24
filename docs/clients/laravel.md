---
title: "LessorMQ for Laravel"
description: "A Laravel queue driver for LessorMQ, built on lessormq/client."
---

# LessorMQ for Laravel

A Laravel queue driver for [LessorMQ](../../README.md), built on
[lessormq/client](php.md). The queue decides what happens
to a failed job (retries, backoff, the dead-letter set); the driver only claims, acks and
fails. Laravel 12 and 13 on PHP 8.2+ (Laravel 13 needs PHP 8.3). Package
`lessormq/laravel`.

## Install

```sh
composer require lessormq/laravel
```

The service provider is discovered on its own and registers the `lessormq` driver. With
ext-pcntl (which `queue:work` needs for its timeouts as well) the worker keeps the lease
of a long job alive. The connection makes HTTP clients of its own (Guzzle, which every
Laravel app has, with a timeout) rather than using the app's, because the lease is
extended from a signal handler while your job may be in the middle of a request of its
own.

## Configure

```php
// config/queue.php, under 'connections'
'lessormq' => [
    'driver' => 'lessormq',
    'url' => env('LESSORMQ_URL'),
    'key' => env('LESSORMQ_KEY'),
    'queue' => env('LESSORMQ_QUEUE', 'default'),
    'wait_seconds' => 5,
    'lease_seconds' => null,
],
```

```ini
# .env
QUEUE_CONNECTION=lessormq
LESSORMQ_URL=https://mq.example.com
LESSORMQ_KEY=lmq_...
```

| Option | Default | |
|---|---|---|
| `url` | (required) | the server |
| `key` | (required) | an API key with the publish and/or consume scope |
| `queue` | `default` | the queue when a job names none |
| `wait_seconds` | 5 | how long a claim waits for a message, 0 to 20 (Redis's `block_for`) |
| `lease_seconds` | the queue's | lease per claim, and per keepalive |
| `keepalive` | true | extend the lease while a job runs (needs ext-pcntl) |
| `consumer` | host:pid | shown to administrators, in the server's list of workers |
| `route` | none | the path this worker runs jobs through (VPN, proxy, account) |
| `http_retries` | 3 | retries after network errors, 5xx and 429 |
| `http_timeout` | 30 | seconds one request may take; at least `wait_seconds` + 10 |
| `after_commit` | false | dispatch after the open database transactions commit |

There is no option for retries, backoff or dead-lettering: set those on the queue
(`PATCH /v1/queues/{queue}`). An option the driver does not know (`retry_after`,
`block_for`) is refused, so a setting copied from another connection does not silently do
nothing.

## Dispatch

Dispatch as always; the payload is stored as JSON, so messages can be read in the queue.

```php
SendInvoice::dispatch($invoice)->onConnection('lessormq')->onQueue('invoices')->delay(60);
```

A job can ask for more or fewer attempts than the queue's default, and for a dedup key
(at most one open message per key), in Laravel's own terms:

```php
class SendInvoice implements ShouldQueue
{
    use Queueable;

    public $tries = 3; // the message's max_attempts, capped at the queue's max_attempts_limit

    public function deduplicationId(string $payload, string $queue): string
    {
        return 'invoice-' . $this->invoice->id;
    }
}
```

`withDeduplicator(fn ($payload, $queue) => ...)` (Laravel 12 and up) works too. `Queue::size()` and
`queue:monitor` count the ready, delayed and processing messages (a key with the consume
scope).

## Run workers

```sh
php artisan queue:work lessormq --queue=invoices --sleep=0
```

The claim itself waits up to `wait_seconds` for a message, so `--sleep=0` costs nothing
and a message published while the worker would sleep is not kept waiting (keep
`wait_seconds` above 0 then). A worker given several queues (`--queue=high,default`)
claims from each in turn, each claim waiting; run a worker per queue for priorities. On
SIGTERM the worker stops after the claim in progress and the job it got, so give your
supervisor a stop timeout above `wait_seconds` plus your longest job.

With ext-pcntl the lease is extended every third of the lease while a job runs, for
`lease_seconds` (or the queue's lease); a failed extend is logged and tried again at the
next one, it does not fail the job. The alarm that does this interrupts `sleep()` in a
job, and it runs between PHP statements: a job stuck in one call (a slow HTTP request) is
extended when that call returns, so give the queue a lease longer than any single call.
Without pcntl there is no keepalive (nor `--timeout`): give the queue a lease longer than
the job, or call `$this->job->extend()` from the job.

## How it behaves

- A job that succeeds: the message is acked.
- A job that throws: the message is failed with the exception's class and message, as
  retryable, or as permanent when the exception implements `LessorMQ\PermanentFailure`
  (throw `LessorMQ\PermanentFailureException`). What follows is the queue's decision.
- `$this->fail()` reports a permanent failure, as do Laravel's own calls to it (a model
  that no longer exists; an exception listed with `dontRetry()`, Laravel 13). The
  queue's `on_permanent` decides: the dead-letter set at once, or another attempt.
- `attempts()` is the server's count: an expired lease counts, a release does not.
- `$tries` (or `tries()`, `#[Tries]`) becomes the message's `max_attempts` when it is
  dispatched. `queue:work --tries`, `$backoff` and `--backoff`, `$maxExceptions`,
  `retryUntil()` and `$failOnTimeout` decide nothing: the worker never fails or delays a
  job on their account, the queue's settings do.
- `$this->release($delay)` gives the message back to the queue for `$delay` seconds
  without counting the attempt, so `RateLimited` and `WithoutOverlapping` postpone a job
  without using up its attempts. `ThrottlesExceptions` turns failures into releases the
  queue does not count: let exceptions through instead.
- A timeout (`--timeout`, `$timeout`) still ends the worker, as always; the attempt is
  first reported as failed with `Illuminate\Queue\TimeoutExceededException`, so the
  queue's backoff applies at once rather than when the lease runs out.
- Laravel's failure handling (the job's `failed()` method, the `catch` callbacks of
  batches and chains, the `JobFailed` event and a row in `failed_jobs`) runs once, when
  the server answers that it gave up on the message (dead-lettered or discarded): not
  after an attempt that will be retried. A message whose last attempt ends with an
  expired lease (the worker died) goes to the dead-letter set without any worker
  knowing, so without a `failed()` call.
- The dead-letter set is the place to look at failed messages and replay them. The row in
  `failed_jobs` is a record; `queue:retry` publishes a copy as a new message, while the
  original stays in the dead-letter set until its retention ends: replay in one place or
  the other, not both.
- A message that is not a Laravel job (published by another client) is failed as
  permanent with the type `LessorMQ\Laravel\InvalidPayload`.
- `queue:clear` is not supported; the queue's messages are managed on the server.

## Moving from another queue

- Let the queue decide about retries: set its `max_attempts` and `backoff` (and
  `on_exhausted`) to what `--tries`, `$backoff` and `retryUntil()` did. `$tries` on a job
  class still works, as the message's `max_attempts`.
- `retry_after` has no equivalent: the worker keeps the lease while the job runs, and
  the queue's `lease_seconds` is how soon a crashed worker's job comes back.
- `block_for` is `wait_seconds`; run the worker with `--sleep=0`.
- `failed_jobs` keeps working, but the dead-letter set is where a failed message lives
  and is replayed; see above.
- Deduplication is `deduplicationId()` or `withDeduplicator()`, as for SQS FIFO queues.
  `ShouldBeUnique` works as before (it is a cache lock, not the queue's).
- Workers on different network paths (VPN exits, proxies) pass theirs as `route`; see
  [routes](../concepts.md#routes).
- Run the old and the new connection side by side and move one job class at a time
  (`$connection` on the job, or `onConnection('lessormq')`), with a worker for each.

## Development

```sh
cd clients/laravel
composer install
vendor/bin/phpunit
```

The tests run Laravel's worker in Orchestra Testbench against a scripted server. The
driver uses lessormq/client from `../php` (a path repository), so a change there is
tested here at once. Another Laravel version: `composer require --dev --no-update
orchestra/testbench:^10.0` (Laravel 12; `^11.0` is 13), then `composer update`.

The protocol calls go through lessormq/client, which passes the
[conformance suite](https://github.com/gsid-nl/lessormq/tree/main/conformance). The
`e2e` group runs the worker against a real server (with `LEASE_SWEEP_SECONDS=1`, as for
the suite), in a tenant of its own that it removes afterwards: a job acked, a failing job
back after the queue's backoff with the server's attempt count until the dead-letter
set, a permanent failure dead-lettered at once, and a job longer than its lease that
keeps it.

```sh
LESSORMQ_URL=http://127.0.0.1:8080 LESSORMQ_OPERATOR_KEY=lmq_... vendor/bin/phpunit --group e2e
```

_From [clients/laravel/README.md](https://github.com/gsid-nl/lessormq/blob/main/clients/laravel/README.md)._
