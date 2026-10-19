-- Lockstep schema. One table is the queue; rows move through
--   queued -> running -> succeeded
--                    \-> queued (retry, after backoff) -> ... -> dead (dead-letter)

create table if not exists jobs (
  id               bigserial primary key,
  queue            text        not null default 'default',
  type             text        not null,
  payload          jsonb       not null default '{}'::jsonb,
  status           text        not null default 'queued'
                   check (status in ('queued', 'running', 'succeeded', 'dead')),
  priority         integer     not null default 0,
  attempts         integer     not null default 0,
  max_attempts     integer     not null default 5 check (max_attempts > 0),
  run_at           timestamptz not null default now(),
  -- Lease: who holds the job and until when. lease_token is a fencing token:
  -- a new one is issued on every claim, and completion only succeeds with the current one.
  lease_token      uuid,
  lease_expires_at timestamptz,
  worker_id        text,
  idempotency_key  text,
  last_error       text,
  result           jsonb,
  created_at       timestamptz not null default now(),
  first_started_at timestamptz,
  completed_at     timestamptz,
  updated_at       timestamptz not null default now()
);

-- The claim query's access path: only queued rows, in dispatch order.
create index if not exists jobs_claim_idx
  on jobs (queue, priority desc, run_at, id)
  where status = 'queued';

-- The reaper's access path: running rows by lease expiry.
create index if not exists jobs_lease_idx
  on jobs (lease_expires_at)
  where status = 'running';

-- Idempotent enqueue: one job per (queue, key).
create unique index if not exists jobs_idempotency_idx
  on jobs (queue, idempotency_key)
  where idempotency_key is not null;

create index if not exists jobs_status_idx on jobs (status, queue);

-- Every attempt (one row per claim), for debugging and for proving what ran where.
-- Keyed by the lease token, which is unique per claim.
create table if not exists job_attempts (
  lease_token uuid        primary key,
  job_id      bigint      not null references jobs(id) on delete cascade,
  attempt     integer     not null,
  worker_id   text        not null,
  started_at  timestamptz not null default now(),
  finished_at timestamptz,
  outcome     text        check (outcome in ('succeeded', 'failed', 'fenced', 'lease_expired', 'released')),
  error       text
);
create index if not exists job_attempts_job_idx on job_attempts (job_id, started_at);

-- Demo side-effect table written by the ledger.credit handler inside the completion
-- transaction. Deliberately has NO unique constraint on job_id, so the tests and the
-- load test can detect a double-applied effect instead of the database hiding it.
create table if not exists ledger (
  id         bigserial primary key,
  job_id     bigint      not null,
  account    text        not null,
  amount     bigint      not null,
  created_at timestamptz not null default now()
);
create index if not exists ledger_job_idx on ledger (job_id);
