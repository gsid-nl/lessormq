---
title: Client libraries
description: "Every LessorMQ library, for each language and framework: how to install it, and where its page is."
---

# Client libraries

Every library does the same few things: publish, claim, run the job, and ack or fail it.
What happens after a failure is never up to the library: the queue's settings decide
whether and when a message comes back, and when it goes to the dead-letter set. A
library only retries a request that did not reach the server, and only where repeating
it is safe.

Each language library has a worker that keeps the lease alive while a job runs and stops
cleanly on a signal, and passes the [conformance suite](https://github.com/gsid-nl/lessormq/tree/main/conformance).
Each framework integration hands the framework's own retries, backoff and failed-job
handling to the queue; its page says, under "How it behaves", what each framework setting
does.

## Languages

| Language | Install |
|---|---|
| [PHP](php.md) | `composer require lessormq/client` |
| [Python](python.md) | `pip install lessormq` |
| [TypeScript and JavaScript](typescript.md) | `npm install lessormq` |
| [Go](go.md) | `go get github.com/gsid-nl/lessormq-go` |
| [Java](java.md) | `dev.lessormq:lessormq` |
| [.NET](dotnet.md) | `dotnet add package LessorMQ` |
| [Ruby](ruby.md) | `gem install lessormq` |

## Frameworks

| Framework | Install |
|---|---|
| [Symfony Messenger](symfony-messenger.md) | `composer require lessormq/messenger` |
| [Laravel](laravel.md) | `composer require lessormq/laravel` |
| [Spring Boot](spring-boot.md) | `dev.lessormq:lessormq-spring-boot-starter` |
| [Django](django.md) | `pip install lessormq-django` |
| [Celery](celery.md) | `pip install lessormq-celery` |
| [Rails](rails.md) | `bundle add lessormq-rails` |

## Another language

The protocol is a handful of HTTP calls, so a worker in a language without a library is
a loop of a few dozen lines: see [Writing a client](../writing-a-client.md). Working
examples for every library are in [Examples](../examples.md).
