---
title: API reference
description: "Every endpoint of the LessorMQ v1 API, with its requests, answers and errors."
---

# API reference

_Generated from [openapi.yaml](https://github.com/gsid-nl/lessorMQ/blob/main/docs/openapi.yaml), which is the source of truth._

LessorMQ leases each message to one worker at a time. Everything that happens to a
message after that is decided by the server, from the settings of its queue: when it
is retried, how long it waits, when it is dead-lettered, which route it may not take
again, how long it is kept.

A client does four things: publish, claim, then ack or fail (and extend a lease while
a long job runs). It may pass settings such as a lease length or a maximum number of
attempts; the server applies them within the limits of the queue. A client never
decides whether a message is retried.

See [the protocol](protocol.md) for the concepts, the message lifecycle and the rules a
conforming client must follow.

Conventions:

- Every timestamp is RFC 3339 in UTC, with a `Z` suffix.
- Every error is `application/problem+json` (RFC 9457) with a stable `code`.
- Clients must ignore response fields they do not know: fields are added within v1,
  never removed or changed in meaning.

## Authentication

`Authorization: Bearer <api key>`.

A tenant key identifies its tenant; there is no tenant in any path outside
`/v1/tenants`. Each tenant key has scopes (`publish`, `consume`, `admin`) and may
be limited to queue name patterns.

An operator key belongs to no tenant. It is for whoever runs the installation (a
web interface, a provisioning tool) and is the only key accepted on `/v1/tenants`.
With the header `LessorMQ-Tenant: <tenant id>` it may call every other endpoint as
an `admin` key of that tenant. The first operator key is created on the command
line; there is no endpoint that creates operator keys.

## Publishing

Put messages on a queue. Requires the `publish` scope.

### Publish a message

`POST /v1/queues/{queue}/messages`

Creates the queue with the tenant's default settings if it does not exist yet.

With a `dedup_key`, a second publish with the same key while the first message is
still open (pending or processing) returns that message with status 200 and
`duplicate: true` instead of creating a new one. Client libraries may generate a
`dedup_key` per publish call so that their own HTTP retries cannot create
duplicates.

Publishing to a paused queue is allowed: pausing only stops claims.

| Parameter | In | Type | Description |
|---|---|---|---|
| `queue` (required) | path | [QueueName](#queuename) | Queue name, unique within the tenant. |

Request body: [PublishRequest](#publishrequest)

| Status | Meaning | Body |
|---|---|---|
| 201 | Message created. | [PublishResponse](#publishresponse) |
| 200 | An open message with the same `dedup_key` exists; it is returned instead. | [PublishResponse](#publishresponse) |
| 400 | The request is malformed or a value is out of range. Code `validation_failed`. | [Problem](#problem) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 413 | The message is larger than the queue's `max_message_bytes`. Code `payload_too_large`. | [Problem](#problem) |
| 429 | The tenant exceeded a quota or rate limit. Code `rate_limited`. | [Problem](#problem) |

## Consuming

Claim messages and report the outcome. Requires the `consume` scope.

### Claim the next message

`POST /v1/queues/{queue}/claims`

Leases the next ready message to the caller and returns it with a `receipt`. Only
the holder of that receipt can extend, ack, fail or release the message, and only
until `lease_until`. After that the lease has expired: the server makes the
message available again and counts the attempt.

With `wait_seconds`, the request waits until a message becomes ready or the time
runs out (long polling). A paused queue behaves as an empty one.

With a `route`, the server will not hand out a message that already failed on
that route, while the queue's `route_policy` says so.

Messages are handed out roughly in order of availability; strict ordering is not
guaranteed. Delivery is at least once: a handler must tolerate seeing a message
again.

| Parameter | In | Type | Description |
|---|---|---|---|
| `queue` (required) | path | [QueueName](#queuename) | Queue name, unique within the tenant. |

Request body (optional): [ClaimRequest](#claimrequest)

| Status | Meaning | Body |
|---|---|---|
| 200 | A message is leased to the caller. | [Lease](#lease) |
| 204 | No message was ready within `wait_seconds`, or the queue is paused. |  |
| 400 | The request is malformed or a value is out of range. Code `validation_failed`. | [Problem](#problem) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 429 | The tenant exceeded a quota or rate limit. Code `rate_limited`. | [Problem](#problem) |

### Extend a lease

`POST /v1/messages/{id}/extend`

Moves `lease_until` to now plus `lease_seconds` (or the queue's default). A worker
calls this while a long job runs; client libraries do it on a timer, at about a
third of the lease.

| Parameter | In | Type | Description |
|---|---|---|---|
| `id` (required) | path | string | Message id as returned by publish or claim. Opaque to clients. |

Request body: [ExtendRequest](#extendrequest)

| Status | Meaning | Body |
|---|---|---|
| 200 | Lease extended. | [ExtendResponse](#extendresponse) |
| 400 | The request is malformed or a value is out of range. Code `validation_failed`. | [Problem](#problem) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |
| 409 | The receipt is not the current lease of this message: the lease expired, the message was claimed again, or it was released. The worker must stop reporting on this message; the server has already made it available again. Code `lease_lost`. | [Problem](#problem) |

### Report success

`POST /v1/messages/{id}/ack`

Marks the message completed. Idempotent per receipt: repeating an ack with the
same receipt returns 204 again.

| Parameter | In | Type | Description |
|---|---|---|---|
| `id` (required) | path | string | Message id as returned by publish or claim. Opaque to clients. |

Request body: [ReceiptRequest](#receiptrequest)

| Status | Meaning | Body |
|---|---|---|
| 204 | Message completed. |  |
| 400 | The request is malformed or a value is out of range. Code `validation_failed`. | [Problem](#problem) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |
| 409 | The receipt is not the current lease of this message: the lease expired, the message was claimed again, or it was released. The worker must stop reporting on this message; the server has already made it available again. Code `lease_lost`. | [Problem](#problem) |

### Report failure

`POST /v1/messages/{id}/fail`

Reports that the attempt failed. The server decides what happens next from the
queue settings and tells the caller in `outcome`; the caller does not act on it.

`permanent: true` is a fact, not a request: it says the input itself cannot be
processed (for example, it cannot be decoded). The queue's `on_permanent` setting
decides whether that skips the remaining attempts.

Idempotent per receipt: repeating a fail with the same receipt returns the same
outcome, as long as the message has not been claimed again.

| Parameter | In | Type | Description |
|---|---|---|---|
| `id` (required) | path | string | Message id as returned by publish or claim. Opaque to clients. |

Request body: [FailRequest](#failrequest)

| Status | Meaning | Body |
|---|---|---|
| 200 | Failure recorded; `outcome` says what the server did. | [FailResponse](#failresponse) |
| 400 | The request is malformed or a value is out of range. Code `validation_failed`. | [Problem](#problem) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |
| 409 | The receipt is not the current lease of this message: the lease expired, the message was claimed again, or it was released. The worker must stop reporting on this message; the server has already made it available again. Code `lease_lost`. | [Problem](#problem) |

### Give a message back without failing it

`POST /v1/messages/{id}/release`

Ends the lease without an outcome, for a worker that has to stop before it can
run the message (shutdown, lost a dependency). The attempt is not counted. The
message is available again after `delay_seconds`.

| Parameter | In | Type | Description |
|---|---|---|---|
| `id` (required) | path | string | Message id as returned by publish or claim. Opaque to clients. |

Request body: [ReleaseRequest](#releaserequest)

| Status | Meaning | Body |
|---|---|---|
| 204 | Message released. |  |
| 400 | The request is malformed or a value is out of range. Code `validation_failed`. | [Problem](#problem) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |
| 409 | The receipt is not the current lease of this message: the lease expired, the message was claimed again, or it was released. The worker must stop reporting on this message; the server has already made it available again. Code `lease_lost`. | [Problem](#problem) |

## Queues

Queue settings and statistics.

### List the tenant's queues

`GET /v1/queues`

Requires the `admin` scope.

| Status | Meaning | Body |
|---|---|---|
| 200 | All queues of the tenant, with settings and statistics. | object |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |

### Get a queue's settings and statistics

`GET /v1/queues/{queue}`

Requires the `consume` or `admin` scope.

| Parameter | In | Type | Description |
|---|---|---|---|
| `queue` (required) | path | [QueueName](#queuename) | Queue name, unique within the tenant. |

| Status | Meaning | Body |
|---|---|---|
| 200 | The queue. | [Queue](#queue) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |

### Change queue settings

`PATCH /v1/queues/{queue}`

Requires the `admin` scope. Only the fields present change. Creates the queue if it
does not exist, so settings can be in place before the first publish.

Pausing is a setting too: `{"paused": true}` stops claims, publishes still land.

| Parameter | In | Type | Description |
|---|---|---|---|
| `queue` (required) | path | [QueueName](#queuename) | Queue name, unique within the tenant. |

Request body: [QueueSettingsPatch](#queuesettingspatch)

| Status | Meaning | Body |
|---|---|---|
| 200 | The queue with its new settings. | [Queue](#queue) |
| 400 | The request is malformed or a value is out of range. Code `validation_failed`. | [Problem](#problem) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |

## Messages

Inspect, replay and delete messages, including the dead-letter set. Requires the `admin` scope.

### List messages of a queue

`GET /v1/queues/{queue}/messages`

Newest first. Paginated with an opaque cursor.

| Parameter | In | Type | Description |
|---|---|---|---|
| `queue` (required) | path | [QueueName](#queuename) | Queue name, unique within the tenant. |
| `status` | query | [MessageStatus](#messagestatus) |  |
| `error_type` | query | string | Only messages whose last error has this `type`. |
| `limit` | query | integer (min 1, max 100) |  |
| `cursor` | query | string | The `next_cursor` of the previous page. |

| Status | Meaning | Body |
|---|---|---|
| 200 | One page of messages. | [MessagePage](#messagepage) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |

### Get a message

`GET /v1/messages/{id}`

| Parameter | In | Type | Description |
|---|---|---|---|
| `id` (required) | path | string | Message id as returned by publish or claim. Opaque to clients. |

| Status | Meaning | Body |
|---|---|---|
| 200 | The message. | [Message](#message) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |

### Delete a message

`DELETE /v1/messages/{id}`

A message that is processing cannot be deleted; wait for its lease to end.

| Parameter | In | Type | Description |
|---|---|---|---|
| `id` (required) | path | string | Message id as returned by publish or claim. Opaque to clients. |

| Status | Meaning | Body |
|---|---|---|
| 204 | Message deleted. |  |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |
| 409 | The message or key is in a state that does not allow this. Code `invalid_state`. | [Problem](#problem) |

### Make a failed or completed message available again

`POST /v1/messages/{id}/replay`

Puts the message back to pending with its attempts reset to zero and its failed
routes cleared. Only from `failed` or `completed`; a pending or processing message
returns 409.

| Parameter | In | Type | Description |
|---|---|---|---|
| `id` (required) | path | string | Message id as returned by publish or claim. Opaque to clients. |

| Status | Meaning | Body |
|---|---|---|
| 200 | Message is pending again. | [Message](#message) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |
| 409 | The message or key is in a state that does not allow this. Code `invalid_state`. | [Problem](#problem) |

### Replay many messages

`POST /v1/queues/{queue}/replay`

Replays every message matching the filter, as `replayMessage` does for one.

| Parameter | In | Type | Description |
|---|---|---|---|
| `queue` (required) | path | [QueueName](#queuename) | Queue name, unique within the tenant. |

Request body: [BulkFilter](#bulkfilter)

| Status | Meaning | Body |
|---|---|---|
| 200 | Number of messages replayed. | [BulkResult](#bulkresult) |
| 400 | The request is malformed or a value is out of range. Code `validation_failed`. | [Problem](#problem) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |

### Delete many messages

`POST /v1/queues/{queue}/purge`

Deletes every message matching the filter. Never touches pending or processing messages.

| Parameter | In | Type | Description |
|---|---|---|---|
| `queue` (required) | path | [QueueName](#queuename) | Queue name, unique within the tenant. |

Request body: [BulkFilter](#bulkfilter)

| Status | Meaning | Body |
|---|---|---|
| 200 | Number of messages deleted. | [BulkResult](#bulkresult) |
| 400 | The request is malformed or a value is out of range. Code `validation_failed`. | [Problem](#problem) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |

## API keys

Keys of the calling tenant. Requires the `admin` scope.

### List the tenant's API keys

`GET /v1/api-keys`

Secrets are never returned after creation; keys are recognised by `prefix`.

| Status | Meaning | Body |
|---|---|---|
| 200 | All keys of the tenant. | object |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |

### Create an API key

`POST /v1/api-keys`

The response is the only time the secret is shown. Store it right away.

Request body: [ApiKeyCreate](#apikeycreate)

| Status | Meaning | Body |
|---|---|---|
| 201 | Key created. | [ApiKeyCreated](#apikeycreated) |
| 400 | The request is malformed or a value is out of range. Code `validation_failed`. | [Problem](#problem) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |

### Revoke an API key

`DELETE /v1/api-keys/{keyId}`

Takes effect immediately. A key cannot revoke itself, so a tenant cannot lock itself out.

| Parameter | In | Type | Description |
|---|---|---|---|
| `keyId` (required) | path | string |  |

| Status | Meaning | Body |
|---|---|---|
| 204 | Key revoked. |  |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |
| 409 | The message or key is in a state that does not allow this. Code `invalid_state`. | [Problem](#problem) |

## Tenant

The calling tenant's own profile, quotas and usage. Requires the `admin` scope.

### Get the calling tenant

`GET /v1/tenant`

| Status | Meaning | Body |
|---|---|---|
| 200 | The tenant the key belongs to. | [Tenant](#tenant) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |

### Change the calling tenant's queue defaults

`PATCH /v1/tenant`

A tenant may change the defaults for its new queues; its name, status and quotas are the operator's.

Request body: object

| Status | Meaning | Body |
|---|---|---|
| 200 | The tenant with its new defaults. | [Tenant](#tenant) |
| 400 | The request is malformed or a value is out of range. Code `validation_failed`. | [Problem](#problem) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |

## Tenants

Create and manage tenants, for a web interface or provisioning tool run by whoever
operates the installation. Requires an operator key.

### List tenants

`GET /v1/tenants`

| Parameter | In | Type | Description |
|---|---|---|---|
| `status` | query | [TenantStatus](#tenantstatus) |  |
| `limit` | query | integer (min 1, max 100) |  |
| `cursor` | query | string | The `next_cursor` of the previous page. |

| Status | Meaning | Body |
|---|---|---|
| 200 | One page of tenants. | object |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |

### Create a tenant

`POST /v1/tenants`

By default also creates a first API key with all three scopes and returns its
secret once, so the interface can hand it to the new tenant.

Request body: [TenantCreate](#tenantcreate)

| Status | Meaning | Body |
|---|---|---|
| 201 | Tenant created. | [TenantCreated](#tenantcreated) |
| 400 | The request is malformed or a value is out of range. Code `validation_failed`. | [Problem](#problem) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 409 | The message or key is in a state that does not allow this. Code `invalid_state`. | [Problem](#problem) |

### Get a tenant

`GET /v1/tenants/{tenantId}`

| Parameter | In | Type | Description |
|---|---|---|---|
| `tenantId` (required) | path | [TenantId](#tenantid) |  |

| Status | Meaning | Body |
|---|---|---|
| 200 | The tenant. | [Tenant](#tenant) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |

### Change a tenant

`PATCH /v1/tenants/{tenantId}`

Suspending a tenant (`status: suspended`) makes every request with its keys return
`403 tenant_suspended`; its messages stay as they are and leases run out normally.
Setting it back to `active` resumes everything.

| Parameter | In | Type | Description |
|---|---|---|---|
| `tenantId` (required) | path | [TenantId](#tenantid) |  |

Request body: [TenantPatch](#tenantpatch)

| Status | Meaning | Body |
|---|---|---|
| 200 | The tenant after the change. | [Tenant](#tenant) |
| 400 | The request is malformed or a value is out of range. Code `validation_failed`. | [Problem](#problem) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |
| 409 | The message or key is in a state that does not allow this. Code `invalid_state`. | [Problem](#problem) |

### Delete a tenant and all its data

`DELETE /v1/tenants/{tenantId}`

Only a suspended tenant can be deleted, so a tenant in use is never removed by one
call. Its status becomes `deleting` and its keys, queues and messages are removed
in the background; the tenant disappears when that is done.

| Parameter | In | Type | Description |
|---|---|---|---|
| `tenantId` (required) | path | [TenantId](#tenantid) |  |

| Status | Meaning | Body |
|---|---|---|
| 202 | Deletion started. | [Tenant](#tenant) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |
| 409 | The message or key is in a state that does not allow this. Code `invalid_state`. | [Problem](#problem) |

### List a tenant's API keys

`GET /v1/tenants/{tenantId}/api-keys`

| Parameter | In | Type | Description |
|---|---|---|---|
| `tenantId` (required) | path | [TenantId](#tenantid) |  |

| Status | Meaning | Body |
|---|---|---|
| 200 | All keys of the tenant. | object |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |

### Create an API key for a tenant

`POST /v1/tenants/{tenantId}/api-keys`

For example when a tenant lost its last admin key. The secret is shown once.

| Parameter | In | Type | Description |
|---|---|---|---|
| `tenantId` (required) | path | [TenantId](#tenantid) |  |

Request body: [ApiKeyCreate](#apikeycreate)

| Status | Meaning | Body |
|---|---|---|
| 201 | Key created. | [ApiKeyCreated](#apikeycreated) |
| 400 | The request is malformed or a value is out of range. Code `validation_failed`. | [Problem](#problem) |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |

### Revoke a tenant's API key

`DELETE /v1/tenants/{tenantId}/api-keys/{keyId}`

| Parameter | In | Type | Description |
|---|---|---|---|
| `tenantId` (required) | path | [TenantId](#tenantid) |  |
| `keyId` (required) | path | string |  |

| Status | Meaning | Body |
|---|---|---|
| 204 | Key revoked. |  |
| 401 | Missing, unknown or revoked API key. Code `unauthorized`. | [Problem](#problem) |
| 403 | The key lacks the scope, the queue is outside its patterns, or a tenant key called an operator endpoint: code `forbidden`. The key's tenant is suspended: code `tenant_suspended`. | [Problem](#problem) |
| 404 | No such queue or message for this tenant. Code `not_found`. | [Problem](#problem) |

## System

Liveness, no authentication.

### Liveness and database reachability

`GET /healthz`

| Status | Meaning | Body |
|---|---|---|
| 200 | The server and its database are reachable. | object |
| 503 | The database or Redis is unreachable. | [Problem](#problem) |

## Schemas

### QueueName

Type: string

### Route

The path a worker runs its jobs through: a VPN exit, a proxy, an API account, a
region. The server records the route of every attempt; with the queue's
`route_policy` set to `avoid_failed`, a message is not handed to a worker on a
route where it failed within the cooldown.

Type: string (up to 255 characters)

### MessageStatus

`pending`: waiting for a worker (ready, or delayed until `available_at`).
`processing`: leased to a worker.
`completed`: acked.
`failed`: out of attempts, or permanently failed; this is the dead-letter set.

Type: string: `pending`, `processing`, `completed`, `failed`

### Headers

Free string metadata, passed through untouched (for example a message type for a framework).

Type: map of string

### PublishRequest

| Field | Type | Description |
|---|---|---|
| `body` (required) | any JSON value | Any JSON value. Passed to the worker exactly as given. |
| `headers` | [Headers](#headers) |  |
| `dedup_key` | string (up to 255 characters) | While a message with this key is open in this queue, publishing again returns that message. |
| `delay_seconds` | integer (min 0) | Make the message available this many seconds from now. Not together with `available_at`. |
| `available_at` | string (date-time) | Make the message available at this time. Not together with `delay_seconds`. |
| `max_attempts` | integer (min 1) | Overrides the queue's `max_attempts` for this message, capped at the queue's `max_attempts_limit`. |

### PublishResponse

| Field | Type | Description |
|---|---|---|
| `id` (required) | string |  |
| `queue` (required) | [QueueName](#queuename) |  |
| `status` (required) | [MessageStatus](#messagestatus) |  |
| `created_at` (required) | string (date-time) |  |
| `available_at` (required) | string (date-time) |  |
| `duplicate` (required) | boolean | True when an open message with the same `dedup_key` was returned instead of a new one. |

### ClaimRequest

| Field | Type | Description |
|---|---|---|
| `consumer` | string (up to 255 characters) | Free identifier of the worker (hostname, pod name), shown in the dashboard. Not used for authorisation. |
| `route` | [Route](#route) |  |
| `wait_seconds` | integer (min 0, max 20) | Wait up to this long for a message instead of returning 204 at once. Default: `0`. |
| `lease_seconds` | integer (min 1) | Lease length for this claim, capped at the queue's `max_lease_seconds`. Defaults to the queue's `lease_seconds`. |

### DeliveredMessage

| Field | Type | Description |
|---|---|---|
| `id` (required) | string |  |
| `queue` (required) | [QueueName](#queuename) |  |
| `body` (required) | any JSON value | The JSON value given at publish. |
| `headers` (required) | [Headers](#headers) |  |
| `attempt` (required) | integer (min 1) | Which attempt this is, counting this one. |
| `max_attempts` (required) | integer (min 1) |  |
| `created_at` (required) | string (date-time) |  |

### Lease

| Field | Type | Description |
|---|---|---|
| `receipt` (required) | string | Proof of this lease. Required to extend, ack, fail or release. Opaque; a new one per claim. |
| `lease_until` (required) | string (date-time) | The lease ends here unless it is extended. |
| `message` (required) | [DeliveredMessage](#deliveredmessage) |  |

### ReceiptRequest

| Field | Type | Description |
|---|---|---|
| `receipt` (required) | string |  |

### ExtendRequest

| Field | Type | Description |
|---|---|---|
| `receipt` (required) | string |  |
| `lease_seconds` | integer (min 1) | New lease length from now, capped at the queue's `max_lease_seconds`. |

### ExtendResponse

| Field | Type | Description |
|---|---|---|
| `lease_until` (required) | string (date-time) |  |

### ErrorReport

| Field | Type | Description |
|---|---|---|
| `type` | string (up to 255 characters) | Machine-readable kind of error, typically the exception class. Used to filter the dead-letter set. |
| `message` (required) | string (up to 65535 characters) |  |
| `details` | any JSON value | Any JSON value with more context (stack trace, response code). Stored, not interpreted. |

### FailRequest

| Field | Type | Description |
|---|---|---|
| `receipt` (required) | string |  |
| `error` (required) | [ErrorReport](#errorreport) |  |
| `permanent` | boolean | The input itself cannot be processed. A fact for the server, which decides what it means via `on_permanent`. Default: `false`. |

### FailResponse

| Field | Type | Description |
|---|---|---|
| `outcome` (required) | string: `retrying`, `dead_lettered`, `discarded` | What the server did. `retrying`: pending again from `available_at`. `dead_lettered`: status `failed`. `discarded`: deleted (queue setting `on_exhausted: discard`). |
| `available_at` | string or null (date-time) |  |
| `attempt` (required) | integer |  |
| `max_attempts` (required) | integer |  |

### ReleaseRequest

| Field | Type | Description |
|---|---|---|
| `receipt` (required) | string |  |
| `delay_seconds` | integer (min 0) | Default: `0`. |

### Message

The full message, as an administrator sees it.

| Field | Type | Description |
|---|---|---|
| `id` (required) | string |  |
| `queue` (required) | [QueueName](#queuename) |  |
| `status` (required) | [MessageStatus](#messagestatus) |  |
| `body` (required) | any JSON value | The JSON value given at publish. |
| `headers` (required) | [Headers](#headers) |  |
| `dedup_key` | string or null |  |
| `attempts` (required) | integer (min 0) | Attempts started so far; an expired lease counts, a release does not. |
| `max_attempts` (required) | integer |  |
| `available_at` (required) | string (date-time) |  |
| `lease_until` | string or null (date-time) |  |
| `consumer` | string or null | The `consumer` of the current or last claim. |
| `last_error` | [RecordedError](#recordederror) or null |  |
| `failed_routes` | map of string (date-time) | Route to the time an attempt last failed on it. |
| `created_at` (required) | string (date-time) |  |
| `updated_at` (required) | string (date-time) |  |
| `completed_at` | string or null (date-time) |  |
| `failed_at` | string or null (date-time) |  |

### RecordedError

All fields of [ErrorReport](#errorreport), and:

| Field | Type | Description |
|---|---|---|
| `at` (required) | string (date-time) |  |
| `attempt` (required) | integer |  |
| `permanent` (required) | boolean |  |
| `expired` | boolean | True when the attempt ended because the lease expired, not by a fail call. |
| `route` | string or null |  |
| `consumer` | string or null |  |

### MessagePage

| Field | Type | Description |
|---|---|---|
| `items` (required) | array of [Message](#message) |  |
| `next_cursor` (required) | string or null |  |

### Backoff

Delay before attempt n+1 after attempt n failed:
min(initial_seconds * multiplier^(n-1), max_seconds), then randomised by +/- jitter.

| Field | Type | Description |
|---|---|---|
| `initial_seconds` | integer (min 0) | Default: `10`. |
| `multiplier` | number (min 1) | Default: `2`. |
| `max_seconds` | integer (min 0) | Default: `3600`. |
| `jitter` | number (min 0, max 1) | Default: `0.1`. |

### RoutePolicy

| Field | Type | Description |
|---|---|---|
| `mode` | string: `none`, `avoid_failed` | `avoid_failed`: do not hand a message to a worker on a route where it failed within `cooldown_seconds`. Default: `"none"`. |
| `cooldown_seconds` | integer (min 0) | How long a failed route stays excluded for that message. When it runs out, the route may be tried again, so a message that failed on every route still makes progress and `max_attempts` ends it.  Default: `3600`. |

### Retention

| Field | Type | Description |
|---|---|---|
| `completed_seconds` | integer (min 0) | Default: `86400`. |
| `failed_seconds` | integer (min 0) | Default: `604800`. |

### QueueSettings

| Field | Type | Description |
|---|---|---|
| `lease_seconds` (required) | integer (min 1) | Default: `300`. |
| `max_lease_seconds` (required) | integer (min 1) | Upper bound for one claim or one extend. A job may run longer as long as its worker keeps extending. Default: `3600`. |
| `max_attempts` (required) | integer (min 1) | Default: `7`. |
| `max_attempts_limit` (required) | integer (min 1) | Highest `max_attempts` a publisher may ask for. Default: `25`. |
| `backoff` (required) | [Backoff](#backoff) |  |
| `on_exhausted` (required) | string: `dead_letter`, `discard` | What happens after the last attempt fails. Default: `"dead_letter"`. |
| `on_permanent` (required) | string: `dead_letter`, `retry` | What a fail with `permanent: true` does. `retry` treats it like any other failure. Default: `"dead_letter"`. |
| `route_policy` (required) | [RoutePolicy](#routepolicy) |  |
| `retention` (required) | [Retention](#retention) |  |
| `max_message_bytes` (required) | integer (min 1) | Upper bound for the encoded `body` plus `headers`. Default: `1048576`. |
| `paused` (required) | boolean | Default: `false`. |

### QueueSettingsPatch

Any subset of the queue settings; nested objects are merged.

| Field | Type | Description |
|---|---|---|
| `lease_seconds` | integer (min 1) |  |
| `max_lease_seconds` | integer (min 1) |  |
| `max_attempts` | integer (min 1) |  |
| `max_attempts_limit` | integer (min 1) |  |
| `backoff` | [Backoff](#backoff) |  |
| `on_exhausted` | string: `dead_letter`, `discard` |  |
| `on_permanent` | string: `dead_letter`, `retry` |  |
| `route_policy` | [RoutePolicy](#routepolicy) |  |
| `retention` | [Retention](#retention) |  |
| `max_message_bytes` | integer (min 1) |  |
| `paused` | boolean |  |

### QueueStats

| Field | Type | Description |
|---|---|---|
| `ready` (required) | integer | Pending and available now. |
| `delayed` (required) | integer | Pending, available later (backoff or delay). |
| `processing` (required) | integer |  |
| `completed` (required) | integer | Completed and not yet removed by retention. |
| `failed` (required) | integer | The dead-letter set. |
| `oldest_ready_seconds` (required) | integer | How long the oldest ready message has been waiting. Grows when no worker takes the queue. |
| `oldest_processing_seconds` (required) | integer |  |

### Queue

| Field | Type | Description |
|---|---|---|
| `name` (required) | [QueueName](#queuename) |  |
| `settings` (required) | [QueueSettings](#queuesettings) |  |
| `stats` (required) | [QueueStats](#queuestats) |  |
| `paused_at` | string or null (date-time) |  |

### BulkFilter

| Field | Type | Description |
|---|---|---|
| `status` (required) | string: `completed`, `failed` |  |
| `error_type` | string | Only messages whose last error has this `type`. |
| `before` | string (date-time) | Only messages that reached their status before this time. |
| `limit` | integer (min 1, max 10000) | Default: `1000`. |

### BulkResult

| Field | Type | Description |
|---|---|---|
| `count` (required) | integer |  |

### Scope

Type: string: `publish`, `consume`, `admin`

### ApiKey

| Field | Type | Description |
|---|---|---|
| `id` (required) | string |  |
| `name` (required) | string |  |
| `prefix` (required) | string | The first characters of the secret, to recognise a key in logs and lists. |
| `scopes` (required) | array of [Scope](#scope) |  |
| `queues` (required) | array of string | Glob patterns of queue names the key may use. Empty means all queues. |
| `created_at` (required) | string (date-time) |  |
| `last_used_at` | string or null (date-time) |  |

### ApiKeyCreate

| Field | Type | Description |
|---|---|---|
| `name` (required) | string (up to 255 characters) |  |
| `scopes` (required) | array of [Scope](#scope) |  |
| `queues` | array of string | Default: `[]`. |

### ApiKeyCreated

All fields of [ApiKey](#apikey), and:

| Field | Type | Description |
|---|---|---|
| `secret` (required) | string | The API key. Shown once. |

### TenantId

Short, stable identifier, chosen at creation. Used in the `LessorMQ-Tenant` header and in metrics.

Type: string

### TenantStatus

Type: string: `active`, `suspended`, `deleting`

### Quotas

Limits per tenant. `null` means unlimited. Exceeding a rate returns `429 rate_limited`.

| Field | Type | Description |
|---|---|---|
| `max_queues` | integer or null (min 1) |  |
| `max_open_messages` | integer or null (min 1) | Pending plus processing, across all queues. Publishing beyond it returns 429. |
| `publish_per_second` | integer or null (min 1) |  |
| `claim_per_second` | integer or null (min 1) |  |
| `max_message_bytes` | integer or null (min 1) | Upper bound for every queue's `max_message_bytes` in this tenant. |
| `max_retention_seconds` | integer or null (min 0) | Upper bound for every queue's retention in this tenant. |

### TenantUsage

| Field | Type | Description |
|---|---|---|
| `queues` (required) | integer |  |
| `open_messages` (required) | integer |  |
| `failed_messages` (required) | integer |  |

### Tenant

| Field | Type | Description |
|---|---|---|
| `id` (required) | [TenantId](#tenantid) |  |
| `name` (required) | string |  |
| `status` (required) | [TenantStatus](#tenantstatus) |  |
| `quotas` (required) | [Quotas](#quotas) |  |
| `queue_defaults` (required) | [QueueSettings](#queuesettings) |  |
| `usage` (required) | [TenantUsage](#tenantusage) |  |
| `created_at` (required) | string (date-time) |  |
| `suspended_at` | string or null (date-time) |  |

### TenantCreate

| Field | Type | Description |
|---|---|---|
| `id` | [TenantId](#tenantid) |  |
| `name` (required) | string (up to 255 characters) |  |
| `quotas` | [Quotas](#quotas) |  |
| `queue_defaults` | [QueueSettingsPatch](#queuesettingspatch) |  |
| `create_admin_key` | boolean | Also create a first key with all three scopes and return it once. Default: `true`. |

### TenantCreated

All fields of [Tenant](#tenant), and:

| Field | Type | Description |
|---|---|---|
| `admin_key` | [ApiKeyCreated](#apikeycreated) or null |  |

### TenantPatch

Only the fields present change. `id` cannot change. `deleting` is set by
`deleteTenant`, never here.

| Field | Type | Description |
|---|---|---|
| `name` | string (up to 255 characters) |  |
| `status` | string: `active`, `suspended` |  |
| `quotas` | [Quotas](#quotas) |  |
| `queue_defaults` | [QueueSettingsPatch](#queuesettingspatch) |  |

### Problem

RFC 9457 problem details, with a stable `code` to branch on.

| Field | Type | Description |
|---|---|---|
| `type` (required) | string (uri) | `urn:lessormq:problem:<code>`. |
| `title` (required) | string |  |
| `status` (required) | integer |  |
| `detail` | string |  |
| `code` (required) | string: `validation_failed`, `unauthorized`, `forbidden`, `tenant_suspended`, `not_found`, `lease_lost`, `invalid_state`, `payload_too_large`, `rate_limited`, `unavailable`, `internal` |  |
| `errors` | array of object | Per-field problems, for `validation_failed`. |

