# Lockstep

A job queue built on PostgreSQL. Producers submit jobs over a REST API; any number of worker processes claim and run them. Workers can crash, stall or be killed at any moment without a job being lost, two workers running the same job at once, or a job's database effect being applied twice.

Java 21 · PostgreSQL 16 · JDBC · Jackson · JUnit 5 · Docker · no framework

```
docker compose up --build --scale worker=3          # API on :8080, 3 worker containers
curl -X POST localhost:8080/jobs -H 'Idempotency-Key: order-42' \
     -d '{"type":"ledger.credit","payload":{"account":"acct-1","amount":250}}'
mvn verify                                          # 40 tests (needs Postgres, see Testing)
java -jar target/lockstep.jar loadtest              # throughput + crash test, writes bench/results.md
```

## Guarantees and how each is enforced

| Guarantee | Mechanism | Proven by |
|---|---|---|
| A job is never claimed by two workers at once | `SELECT … FOR UPDATE SKIP LOCKED` inside the claim | `ConcurrentClaimTest`: 16 threads racing over 3,000 jobs, 0 double claims. Without `SKIP LOCKED` the same test sees 12,688. |
| A crashed worker's jobs are not lost | Claims are **leases**. Workers renew them by heartbeat; a reaper requeues jobs whose lease lapsed | `reaperRequeuesJobsWhoseWorkerDied`; load test with 3 × `SIGKILL` |
| A stalled "zombie" worker can't overwrite the real result | Every claim issues a new **fencing token**; completion only counts with the current one | `fencingRejectsAZombieWorkersLateCompletion`, `aStalledWorkerIsFencedAndTheCreditIsNotDoubled` |
| A job's database effect is applied exactly once | The effect commits **in the same transaction** as the fenced completion | Load test: 11,337 credits, 0 applied twice, 0 missing |
| Resubmitting a request doesn't create a duplicate job | Unique index on `(queue, idempotency_key)` + `INSERT … ON CONFLICT DO NOTHING` | `concurrentDuplicateSubmissionsCreateOneJob`: 12 clients racing, 1 job |
| Failures retry without a thundering herd | Exponential backoff with full jitter; dead-letter after `maxAttempts` | `RetryPolicyTest`, `failuresRetryUntilAttemptsRunOutThenDeadLetter` |
| Shutdown doesn't strand work | `SIGTERM` → stop claiming, finish in-flight jobs, hand back the rest without charging an attempt | `gracefulShutdownHandsUnfinishedJobsBack` |

A note on "exactly once": **delivery** is at-least-once. If a worker dies mid-job, the job runs again elsewhere. That can't be avoided without coordinating with whatever the job talks to. What Lockstep guarantees is that **effects written through the completion transaction** happen exactly once, and that a job's code never runs on two live leases at the same time. Effects outside the database (sending an email, calling an API) should be made idempotent, for example by passing the job id as an idempotency key downstream.

## How it works

```mermaid
stateDiagram-v2
    [*] --> queued: enqueue
    queued --> running: claim (new lease + fencing token)
    running --> succeeded: complete (token matches)
    running --> queued: fail, attempts left (run_at = now + backoff)
    running --> queued: lease expired (reaper)
    running --> dead: fail, out of attempts / permanent error
    dead --> queued: POST /jobs/{id}/retry
    succeeded --> [*]
```

**Claiming** is one round trip. A data-modifying CTE locks the next ready rows (skipping any another worker has locked), marks them running with a fresh lease token, and logs the attempt:

```sql
with next as (
  select id from jobs
  where status = 'queued' and queue = ? and run_at <= now()
  order by priority desc, run_at, id
  limit ? for update skip locked
), claimed as (
  update jobs j set status = 'running', attempts = j.attempts + 1,
         lease_token = gen_random_uuid(), lease_expires_at = now() + ? * interval '1 second', …
  from next where j.id = next.id returning j.*
), logged as (
  insert into job_attempts (lease_token, job_id, attempt, worker_id) select … from claimed
) select … from claimed;
```

A partial index on `(queue, priority desc, run_at, id) where status = 'queued'` makes this an index scan that touches only ready jobs, however large the table grows.

**Completing** is a transaction: `update jobs set status = 'succeeded' … where id = ? and lease_token = ?`. If no row matches, the worker has lost the job and everything rolls back. If it matches, the row is now locked, so the reaper can't take it mid-commit, and the handler's effect runs in the same transaction.

**Leases.** A heartbeat renews leases every `lease / 3`, but stops renewing a job that has run past `maxRuntime`, so a hung handler eventually loses its job instead of holding it forever. Every worker process runs the reaper. Reapers use `SKIP LOCKED` too, so they never block each other or the claimers.

**Polling.** Idle workers back off exponentially with jitter (20 ms up to 1 s), so an idle fleet costs a few cheap index lookups per second.

## Results

From `java -jar target/lockstep.jar loadtest`, on a 2-core VM with PostgreSQL, the worker processes and the load generator all on the same machine. Full output in [`bench/results.md`](bench/results.md).

**Chaos run:** 4 worker processes × 4 threads, 300 jobs/s for 40 s, 5% duplicate submissions, 5% transient failures, 1% permanently broken jobs, 3 workers killed with `SIGKILL` and replaced.

| | |
|---|---|
| Jobs created / expected | 11,467 / 11,467 (533 duplicate submissions collapsed) |
| Succeeded · dead-lettered · unfinished | 11,337 · 130 (exactly the poison jobs) · 0 |
| Ledger credits applied twice · missing | **0 · 0** |
| Latency p50 · p95 (first-attempt jobs) | 11 ms · 24 ms |
| Worst latency (job caught in a crash, waits out the 3 s lease) | 4.9 s |

**Drain throughput** (20,000 jobs, 2 ms simulated work + a DB write each): 566 jobs/s with 1 process, 715 with 2. It flattens beyond that because two cores are shared by PostgreSQL and every worker JVM. Run it on more cores for numbers that mean something for your hardware.

### A bug the load test found

The first version claimed with `queue = any(?::text[])` so one query could serve several queues. The first load test showed about 200 jobs/s with no gain from adding workers. `EXPLAIN ANALYZE` showed why: `= ANY(array)` can't use the partial index's sort order, so **every claim was a sequential scan and sort of the whole table** (7.7 ms on 20,000 queued jobs). Claiming one queue per query with `queue = ?` brought it back to an index scan (0.08 ms) and throughput rose **2.8×**. Multiple queues are now served by rotating through them, which also stops a busy queue starving a quiet one (`severalQueuesAreServedInRotationSoNoneStarves`).

## Design decisions

* **Why PostgreSQL rather than Redis or Kafka?** If the application already uses Postgres, the queue gets transactions, durability and backups for free, and a job can commit atomically with the business data it changes. That atomicity is how the exactly-once effect works. The cost is throughput: a dedicated broker handles far more messages per second. Lockstep suits thousands of jobs per second, not millions.
* **Leases and fencing rather than holding a row lock while the job runs.** A transaction held open for a whole job ties up a connection, blocks vacuum, and if the network drops, the job's fate is unknown. Leases make every state change short, and fencing makes late writers harmless.
* **Polling rather than `LISTEN/NOTIFY`.** Polling with backoff is simple and robust across connection drops. `NOTIFY` would cut idle-to-busy latency (now up to 1 s) and is the obvious next step.
* **No framework.** The JDK's `HttpServer` with a virtual thread per request covers seven routes. The whole service is about 2,000 lines of Java with two runtime dependencies: the Postgres driver and Jackson.

## API

| Method | Path | Notes |
|---|---|---|
| `POST` | `/jobs` | `{type, payload?, queue?, priority?, maxAttempts?, delaySeconds?, idempotencyKey?}`. The `Idempotency-Key` header also works. `201` if created, `200` with the existing job if the key was seen before. |
| `GET` | `/jobs/{id}` | |
| `GET` | `/jobs?status=&queue=&limit=` | Newest first |
| `POST` | `/jobs/{id}/retry` | Dead-letter → queue. `409` if the job isn't dead. |
| `GET` | `/stats` | Counts by queue and status, completions per minute, latency p50/p95/p99, age of the oldest ready job |
| `GET` | `/metrics` | The same in Prometheus format |
| `GET` | `/health` | |

Job types are plain Java:

```java
registry.register("invoice.send", job -> {
    Invoice inv = parse(job.payload());
    String messageId = mailer.send(inv, /* idempotency key */ "job-" + job.id());
    return Outcome.withEffect("{\"messageId\":\"" + messageId + "\"}", tx -> markInvoiceSent(tx, inv.id()));
});
```

Throw `NonRetryableException` for failures retrying can't fix (bad input): the job goes straight to the dead-letter queue.

## Running

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | `jdbc:postgresql://localhost:5432/lockstep` | JDBC or `postgres://user:pass@host/db` |
| `DB_USER` / `DB_PASSWORD` | `lockstep` / `lockstep` | |
| `PORT` | `8080` | API |
| `WORKER_THREADS` | `4` | per process |
| `QUEUES` | `default` | comma-separated |
| `LEASE_SECONDS` | `30` | |

`java -jar lockstep.jar api | worker | all | migrate | loadtest`. The schema is applied on startup, guarded by an advisory lock so parallel starts don't race.

## Testing

```
docker compose up -d db
mvn verify
```

The tests run against a real PostgreSQL database (`TEST_DATABASE_URL`, default the compose database) because the behaviour under test (row locks, `SKIP LOCKED`, `ON CONFLICT`, transaction rollback) is the database's. 40 tests cover the store's state transitions, racing claimers and racing duplicate submissions, the worker pool (flaky jobs, dead-lettering, a stalled zombie worker, heartbeats, graceful shutdown) and the HTTP API. CI also runs a short load + crash test and fails the build if an invariant breaks.

## Limitations

* No per-job timeout that interrupts a hung handler: it loses its lease after `maxRuntime` and the job runs elsewhere, but the thread stays busy.
* Priorities apply within a queue. Across queues, workers rotate.
* Throughput is bounded by PostgreSQL write capacity. Every job costs one claim and one completion transaction.
* `/stats` scans the jobs table. Fine for a demo; at scale it should read from a summary table or the metrics pipeline.

## Layout

```
src/main/java/io/github/mathais4/lockstep/
  core/     JobStore (all state transitions), Job, NewJob, RetryPolicy, Outcome, JobHandler
  worker/   WorkerPool (claim loop, heartbeat, reaper, shutdown), HandlerRegistry, DemoHandlers
  api/      ApiServer
  db/       ConnectionPool, Migrator
  bench/    LoadTest (multi-process drain + SIGKILL chaos test)
src/main/resources/db/V1__jobs.sql
src/test/java/…   40 tests
bench/results.md   latest load-test output
```
