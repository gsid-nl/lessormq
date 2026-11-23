# Load test

`loadtest` drives a LessorMQ server with publishers and workers and reports throughput,
latency per call, the time from publish to ack, and errors. Publishers publish at a fixed
total rate: when the server cannot keep up they fall behind (reported), rather than the
offered load dropping to whatever the server manages.

```sh
cd tools/loadtest
go run . -url http://127.0.0.1:18080 -key lmq_... -rate 300 -workers 50 -duration 30m
go run . -url ... -key ... -burst 10000 -publishers 50 -workers 40
go run . -h
```

Use a tenant of its own: the messages stay in the queue's completed set for its retention.
With `-batch N` every worker claims up to N messages at once (`max_messages`) and acks them
together (`POST /v1/acks`).

The results from before 2026-11-23 are of the PHP server LessorMQ had until 0.4.

`run-local.sh` starts an installation shaped like a small production one (`compose.yaml`:
two application nodes with the server image, 1 CPU each, behind nginx; MariaDB and Redis),
runs the scenarios below against it, and removes it:

```sh
tools/loadtest/run-local.sh                    # every scenario, 2 minutes each
DURATION=30m tools/loadtest/run-local.sh tenfold
```

| Scenario | Load |
|---|---|
| `normal` | 33 publishes a second, 21 workers claiming with a 2 s wait |
| `tenfold` | 330 a second, 40 workers |
| `burst` | 10,000 publishes at once, 50 publishers, 40 workers |
| `waiting` | 33 a second, 60 workers claiming with a 10 s wait |
| `overload` | 100 publishers and 100 workers as fast as they can |

## Results

### The Go server on gsid's hetzner cluster, 2026-11-23

The setup of the run below, with the Go server (image of commit 1a3a670) in place of PHP
and nginx: two pods of 1 CPU, Redis for the two of them, the same MariaDB of at most 4 CPU.
3 minutes per scenario. Latencies in ms: p50 / p95 / p99.

| Scenario | Done per second | Errors | publish | ack | publish to ack |
|---|---|---|---|---|---|
| normal | 33 (all of it) | 0 | 1.6 / 2.1 / 3.1 | 0.9 / 1.1 / 1.4 | 3.9 / 4.8 / 9.1 |
| tenfold | 330 (all of it) | 0 | 0.7 / 3.4 / 6.8 | 0.5 / 0.7 / 3.4 | 1.8 / 3.3 / 9.1 |
| burst | 10,000 in 9 s | 0 | 2.9 / 70 / 77 | 2.5 / 68 / 73 | 3.7 s at the median |
| waiting | 33 (all of it) | 0 | 1.6 / 2.0 / 2.6 | 0.6 / 1.1 / 1.2 | 3.6 / 4.7 / 5.4 |
| overload, 2 pods | 2,300 to 2,700 published, 470 to 670 acked | 0 | 13 / 94 / 107 | 14 / 94 / 105 | the queue grows |

- Ten times today's peak runs in milliseconds on two pods of 1 CPU, which PHP could not
  keep up with (180 acks a second). A burst of 10,000 is done in 9 s instead of 45.
- In overload the database is the limit: MariaDB at its 4 CPUs, the Go pods at a quarter
  of theirs. Past that it degrades instead of levelling off: with 4 pods (more pressure on
  the same database) 132 acks a second, with batches of 20 (`-batch 20`) 348. Locally,
  where MariaDB has no CPU limit, batches were clearly faster; on a database held to 4 CPUs
  under 200 clients that go as fast as they can, they were not. Unexplained so far.
- So the next measurement is a fixed rate stepped up (500, 1,000, 1,500 a second) rather
  than as fast as possible, on a database without a CPU limit as production has.

### gsid's hetzner cluster with a database of its own, 2026-11-23

What the PHP server itself does on hetzner hardware when the database is not the limit:
a test instance in a namespace of its own, two pods of the production shape (image 0.3.1,
1 CPU each, 16 FPM workers and the claims pool), its own MariaDB 10.11 (one node, no
Galera, `innodb_flush_log_at_trx_commit=2` as production now has, at most 4 CPU) and
Redis, the load generator in the cluster. 3 minutes per scenario; the last one again with
4 pods. Latencies in ms: p50 / p95 / p99.

| Scenario | Done per second | Errors | publish | ack | publish to ack |
|---|---|---|---|---|---|
| normal | 33 (all of it) | 0 | 5.5 / 6.6 / 7.8 | 3.3 / 4.9 / 5.5 | 10.6 / 13.7 / 16.4 |
| tenfold | about 290 published, 180 acked of 330 asked | 0 | 105 / 292 / 299 | 99 / 202 / 295 | the queue grows |
| burst | 222 (10,000 in 45 s) | 0 | 108 / 198 / 201 | 99 / 196 / 199 | 13.5 s at the median |
| waiting | 33 (all of it) | 0 | 5.5 / 6.4 / 7.0 | 4.1 / 5.0 / 5.4 | 11.7 / 13.9 / 14.9 |
| overload, 2 pods | 209 full cycles | 0 | 399 / 703 / 802 | 392 / 697 / 799 | the queue grows |
| overload, 4 pods | 420 full cycles | 0 | 194 / 393 / 495 | 105 / 299 / 401 | the queue grows |

- The limit is PHP's CPU: past `normal` each php container sat at its 1 CPU limit, and the
  latencies come in steps of 100 ms, the CFS period in which a throttled container waits
  for its next share. About 105 full cycles (publish, claim, ack) a second per CPU, 9.5 ms
  of CPU per message, and it scales linearly: 4 pods do twice what 2 do.
- The database had room: MariaDB used 0.85 CPU at 209 cycles a second and 1.9 CPU at 420,
  about 4.5 ms of its CPU per message; no deadlocks, Redis under 0.1 CPU. A message takes
  2 commits and about 19 statements (`Questions` per acked message).
- Today's peak (33 a second) and 60 waiting workers run in milliseconds. Ten times the peak
  needs 4 such pods, or less CPU per message.
- The 2026-11-21 run below was limited by the shared Galera cluster flushing every commit;
  it has since moved to `innodb_flush_log_at_trx_commit=2`, so that limit is out of date.

### gsid's hetzner cluster, 2026-11-21

A test instance next to production: two pods of the production shape (1 CPU, 16 FPM
workers for most calls and the claims pool), the production Galera cluster (3 MariaDB
nodes) and Redis, the load generator in the cluster. 5 minutes per scenario; no overload
scenario, as the database is shared. Latencies in ms: p50 / p95 / p99.

| Scenario | Done per second | Errors | publish | ack | publish to ack |
|---|---|---|---|---|---|
| normal | 33 (all of it) | 0 | 9 / 40 / 61 | 7 / 33 / 49 | 21 / 97 / 138 |
| tenfold | 86 of 330 asked | 0 | 470 / 770 / 895 | 371 / 741 / 869 | the queue grows |
| burst | 74 (10,000 in 3.5 minutes) | 0 | 623 / 1,103 / 2,464 | 403 / 1,004 / 1,783 | 61 s at the median |
| waiting | 33 (all of it) | 0 | 13 / 36 / 618 | 11 / 27 / 546 | 33 / 76 / 1,841 |

The limit is the database's commits, not LessorMQ: the application pods used at most half
a CPU each, MariaDB's primary a quarter, and a round trip to the database takes 0.16 ms,
to Redis 0.33 ms. One commit takes 3.6 ms without load (each node writes it to disk,
`innodb_flush_log_at_trx_commit=1`, and Galera certifies it on all three), and Galera
commits one after the other: about 280 commits a second for the whole cluster. A message
takes three (publish, claim, ack), so about 90 messages a second; during the tenfold run a
commit took 216 ms, for every application on the cluster.

### Local, 2026-11-21

On the installation of `compose.yaml` (2 × 1 CPU for the application; MariaDB not
limited, one node), 1 minute per scenario, before the claims pool. Latencies in ms:
p50 / p95 / p99.

| Scenario | Done per second | Errors | publish | claim (with a message) | ack | publish to ack |
|---|---|---|---|---|---|---|
| normal | 33 (all of it) | 0 | 4 / 8 / 682 | waits for work | 4 / 7 / 133 | 11 / 75 / 810 |
| tenfold | 265 of 330 asked | 0 | 66 / 475 / 491 | 80 / 397 / 488 | 73 / 395 / 484 | 5,459 / 23,194 / 23,696 |
| burst | 469 | 0 | 94 / 277 / 282 | 85 / 279 / 290 | 85 / 254 / 286 | all 10,000 done in 21 s |
| waiting | 3 of 33 asked | 0 | 9,533 / 13,587 / 13,820 | 9,978 / 13,919 / 13,986 | 73 / 10,098 / 10,142 | 9,974 / 20,179 / 23,761 |
| overload | 329 | 0 | 300 / 896 / 1,098 | 176 / 707 / 1,013 | 114 / 700 / 1,006 | queue grows |

- Two application nodes of 1 CPU do about 300 full cycles (publish, claim, ack) a second;
  past that the latency grows but nothing fails.
- Every waiting claim holds a PHP-FPM worker. With more waiting workers than FPM workers
  (60 against 2 × 16), publishes and acks wait behind them for seconds: `waiting`. With
  the claims pool (docker/claims.conf) the same scenario gives publish 4.6 / 6.5 / 7.5.
