# Lockstep load test

2026-10-19 · Java 21.0.12.1 · 2 CPU cores · PostgreSQL on the same machine

## Drain: throughput by worker processes

20,000 `ledger.credit` jobs queued up front, each doing 2 ms of simulated work, then a database write in the completion transaction. 4 threads per worker process. Timed from first claim to last completion.

| Worker processes | Threads | Jobs/s | Wall time | p95 claim-to-done | Ledger rows | Duplicate effects |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 4 | 566 | 35.4 s | 7 ms | 20,000 | 0 |
| 2 | 8 | 715 | 28.0 s | 10 ms | 20,000 | 0 |
| 4 | 16 | 573 | 34.9 s | 26 ms | 20,000 | 0 |

## Chaos: steady load with workers killed mid-run

4 worker processes × 4 threads. 300 jobs/s submitted for 40 s through `JobStore.enqueue` (12,000 submissions in 40.0 s). 5% of submissions repeat an earlier idempotency key, 5% of ledger jobs fail transiently on each attempt, 1% of jobs are permanently broken. 3 workers killed with SIGKILL at evenly spaced points and replaced. Lease 3 s.

| Measure | Result |
|---|---|
| Submissions (incl. duplicates) | 12,000 (533 duplicates) |
| Jobs created | 11,467 (expected 11,467: duplicates collapsed) |
| Succeeded | 11,337 |
| Dead-lettered | 130 (all 130 poison jobs, on their first attempt) |
| Unfinished | 0 |
| Jobs that needed a retry | 581 |
| Attempts lost to killed workers, recovered by the reaper | 2 |
| Late completions rejected by fencing | 0 |
| Latency, enqueue to done, all jobs: p50 / p95 / p99 | 11 / 197 / 874 ms |
| Latency, jobs that succeeded first time: p95 / p99 | 24 / 52 ms (the rest include retry backoff) |
| Latency, worst (a job caught in a crash waits out the lease) | 4.88 s |
| Ledger credits applied twice | **0** |
| Succeeded jobs with no ledger credit | **0** |
| Ledger credits for dead-lettered jobs | **0** |

**Invariants: all held**
