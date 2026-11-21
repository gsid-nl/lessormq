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

`run-local.sh` starts an installation shaped like a small production one (`compose.yaml`:
two application nodes with the production image, PHP-FPM with 16 workers, 1 CPU and 1 GiB
each, behind nginx; MariaDB and Redis), runs the scenarios below against it, and removes
it:

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
