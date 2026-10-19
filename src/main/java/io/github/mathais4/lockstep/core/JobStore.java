package io.github.mathais4.lockstep.core;

import io.github.mathais4.lockstep.db.ConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * All queue state transitions, each as one SQL statement or one short transaction.
 *
 * <h2>Guarantees</h2>
 * <ul>
 *   <li><b>No double claiming.</b> {@link #claim} locks candidate rows with
 *   {@code FOR UPDATE SKIP LOCKED}, so concurrent workers never pick the same job and never
 *   wait on each other.</li>
 *   <li><b>Crash recovery.</b> A claim is a lease. Workers renew it while working
 *   ({@link #heartbeat}); if a worker dies, {@link #reapExpired} puts the job back.</li>
 *   <li><b>Fencing.</b> Every claim issues a new lease token, and {@link #complete} /
 *   {@link #fail} only take effect with the current token. A worker that stalled past its lease
 *   (a "zombie") cannot overwrite the result of the worker that took over.</li>
 *   <li><b>Exactly-once database effects.</b> A handler's {@link Outcome#effect()} commits in the
 *   same transaction as the fenced completion, so it is applied once even though the job's code
 *   may run more than once.</li>
 * </ul>
 */
public final class JobStore {

    public record EnqueueResult(Job job, boolean created) {}

    public enum FailResult { RETRY_SCHEDULED, DEAD, FENCED }

    public record Reaped(int requeued, int dead) {}

    public record QueueStats(
            Map<String, Map<String, Long>> countsByQueue,
            long succeededLastMinute,
            Double latencyP50Seconds,
            Double latencyP95Seconds,
            Double latencyP99Seconds,
            Double oldestReadySeconds) {}

    private static final int MAX_ERROR_LENGTH = 2000;

    private final ConnectionPool pool;
    private final RetryPolicy retryPolicy;
    private final AtomicInteger rotation = new AtomicInteger();

    public JobStore(ConnectionPool pool, RetryPolicy retryPolicy) {
        this.pool = pool;
        this.retryPolicy = retryPolicy;
    }

    public ConnectionPool pool() {
        return pool;
    }

    // ------------------------------------------------------------------ enqueue

    /**
     * Adds a job. With an idempotency key, submitting the same (queue, key) again returns the
     * existing job instead of creating a duplicate, even when both requests race.
     */
    public EnqueueResult enqueue(NewJob j) throws SQLException {
        return pool.withConnection(c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "insert into jobs (queue, type, payload, priority, max_attempts, run_at, idempotency_key) "
                            + "values (?, ?, ?::jsonb, ?, ?, now() + ? * interval '1 second', ?) "
                            + "on conflict (queue, idempotency_key) where idempotency_key is not null do nothing "
                            + "returning " + Job.COLUMNS)) {
                ps.setString(1, j.queue());
                ps.setString(2, j.type());
                ps.setString(3, j.payload());
                ps.setInt(4, j.priority());
                ps.setInt(5, j.maxAttempts());
                ps.setDouble(6, j.delaySeconds());
                ps.setString(7, j.idempotencyKey());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) return new EnqueueResult(Job.from(rs), true);
                }
            }
            // Conflict: the key already exists. ON CONFLICT waited for any in-flight insert to commit.
            try (PreparedStatement ps = c.prepareStatement(
                    "select " + Job.COLUMNS + " from jobs where queue = ? and idempotency_key = ?")) {
                ps.setString(1, j.queue());
                ps.setString(2, j.idempotencyKey());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) throw new SQLException("idempotent insert conflicted but no row found");
                    return new EnqueueResult(Job.from(rs), false);
                }
            }
        });
    }

    // ------------------------------------------------------------------ claim

    /**
     * Claims up to {@code limit} ready jobs. With several queues, the starting queue rotates on
     * each call so no queue starves; within a queue, highest priority first, then oldest.
     *
     * <p>Each queue is claimed with its own {@code queue = ?} query rather than one
     * {@code queue = any(?)} query: the latter can't use the partial claim index, which turned
     * every claim into a sequential scan and sort of the whole table (7.7 ms vs 0.08 ms on
     * 20,000 queued jobs, measured with EXPLAIN ANALYZE).
     */
    public List<Job> claim(Collection<String> queues, String workerId, int limit, Duration lease) throws SQLException {
        if (queues.isEmpty()) throw new IllegalArgumentException("no queues");
        List<String> qs = List.copyOf(queues);
        for (String q : qs) {
            if (!NewJob.NAME.matcher(q).matches()) throw new IllegalArgumentException("bad queue name " + q);
        }
        int start = qs.size() == 1 ? 0 : Math.floorMod(rotation.getAndIncrement(), qs.size());
        for (int i = 0; i < qs.size(); i++) {
            List<Job> got = claimFrom(qs.get((start + i) % qs.size()), workerId, limit, lease);
            if (!got.isEmpty()) return got;
        }
        return List.of();
    }

    /** One round trip: lock the next ready rows, mark them running under a fresh lease token, log the attempts. */
    private List<Job> claimFrom(String queue, String workerId, int limit, Duration lease) throws SQLException {
        return pool.withConnection(c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "with next as ("
                            + "  select id from jobs"
                            + "  where status = 'queued' and queue = ? and run_at <= now()"
                            + "  order by priority desc, run_at, id"
                            + "  limit ?"
                            + "  for update skip locked"
                            + "), claimed as ("
                            + "  update jobs j set status = 'running', attempts = j.attempts + 1,"
                            + "    lease_token = gen_random_uuid(), lease_expires_at = now() + ? * interval '1 second',"
                            + "    worker_id = ?, first_started_at = coalesce(j.first_started_at, now()), updated_at = now()"
                            + "  from next where j.id = next.id"
                            + "  returning j.*"
                            + "), logged as ("
                            + "  insert into job_attempts (lease_token, job_id, attempt, worker_id)"
                            + "  select lease_token, id, attempts, worker_id from claimed"
                            + ") select " + Job.COLUMNS + " from claimed order by priority desc, run_at, id")) {
                ps.setString(1, queue);
                ps.setInt(2, limit);
                ps.setDouble(3, seconds(lease));
                ps.setString(4, workerId);
                List<Job> out = new ArrayList<>(limit);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) out.add(Job.from(rs));
                }
                return out;
            }
        });
    }

    // ------------------------------------------------------------------ finish

    /**
     * Marks a claimed job succeeded and applies its effect, atomically, if and only if the
     * caller still holds the lease. Returns false if the job was fenced (lease lost).
     */
    public boolean complete(Job job, Outcome outcome) throws SQLException {
        boolean done = pool.inTransaction(tx -> {
            try (PreparedStatement ps = tx.prepareStatement(
                    "update jobs set status = 'succeeded', result = ?::jsonb, completed_at = now(),"
                            + " lease_token = null, lease_expires_at = null, last_error = null, updated_at = now()"
                            + " where id = ? and lease_token = ?::uuid and status = 'running'")) {
                ps.setString(1, outcome.resultJson());
                ps.setLong(2, job.id());
                ps.setString(3, job.leaseToken().toString());
                if (ps.executeUpdate() == 0) return false;
            }
            // The row is now locked by this transaction, so the reaper cannot take it mid-commit.
            if (outcome.effect() != null) outcome.effect().apply(tx);
            markAttempt(tx, job.leaseToken(), "succeeded", null);
            return true;
        });
        if (!done) recordFenced(job, "lease lost before completion; result discarded");
        return done;
    }

    /** Records a failed attempt: schedules a retry with backoff, or dead-letters the job. */
    public FailResult fail(Job job, Throwable error, boolean retryable) throws SQLException {
        String message = describe(error);
        double delay = retryPolicy.delaySeconds(job.attempts(), ThreadLocalRandom.current());
        FailResult r = pool.inTransaction(tx -> {
            String status;
            try (PreparedStatement ps = tx.prepareStatement(
                    "update jobs set"
                            + " status = case when ? or attempts >= max_attempts then 'dead' else 'queued' end,"
                            + " run_at = case when ? or attempts >= max_attempts then run_at else now() + ? * interval '1 second' end,"
                            + " completed_at = case when ? or attempts >= max_attempts then now() else null end,"
                            + " last_error = ?, lease_token = null, lease_expires_at = null, updated_at = now()"
                            + " where id = ? and lease_token = ?::uuid and status = 'running'"
                            + " returning status")) {
                ps.setBoolean(1, !retryable);
                ps.setBoolean(2, !retryable);
                ps.setDouble(3, delay);
                ps.setBoolean(4, !retryable);
                ps.setString(5, message);
                ps.setLong(6, job.id());
                ps.setString(7, job.leaseToken().toString());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) return FailResult.FENCED;
                    status = rs.getString(1);
                }
            }
            markAttempt(tx, job.leaseToken(), "failed", message);
            return "dead".equals(status) ? FailResult.DEAD : FailResult.RETRY_SCHEDULED;
        });
        if (r == FailResult.FENCED) recordFenced(job, "lease lost before failure was recorded: " + message);
        return r;
    }

    // ------------------------------------------------------------------ leases

    /** Extends the leases of jobs this worker is still running. Returns how many were extended. */
    public int heartbeat(Collection<UUID> leaseTokens, Duration lease) throws SQLException {
        if (leaseTokens.isEmpty()) return 0;
        return pool.withConnection(c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "update jobs set lease_expires_at = now() + ? * interval '1 second', updated_at = now()"
                            + " where status = 'running' and lease_token = any(?::uuid[])")) {
                ps.setDouble(1, seconds(lease));
                ps.setString(2, pgArray(leaseTokens.stream().map(UUID::toString).toList()));
                return ps.executeUpdate();
            }
        });
    }

    /**
     * Returns jobs whose lease expired (their worker crashed or stalled) to the queue, or
     * dead-letters them if that was their last attempt. Safe to run from every worker at once.
     */
    public Reaped reapExpired(int limit) throws SQLException {
        return pool.withConnection(c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "with expired as ("
                            + "  select id, lease_token from jobs where status = 'running' and lease_expires_at < now()"
                            + "  order by lease_expires_at limit ? for update skip locked"
                            + "), upd as ("
                            + "  update jobs j set"
                            + "    status = case when j.attempts >= j.max_attempts then 'dead' else 'queued' end,"
                            + "    completed_at = case when j.attempts >= j.max_attempts then now() else null end,"
                            + "    run_at = now(),"
                            + "    last_error = 'lease expired: worker ' || coalesce(j.worker_id, '?') || ' stopped renewing it',"
                            + "    lease_token = null, lease_expires_at = null, updated_at = now()"
                            + "  from expired where j.id = expired.id"
                            + "  returning j.status, expired.lease_token as old_token"
                            + "), att as ("
                            + "  update job_attempts a set outcome = 'lease_expired', finished_at = now()"
                            + "  from upd where a.lease_token = upd.old_token and a.outcome is null"
                            + ") select count(*) filter (where status = 'queued') as requeued,"
                            + "         count(*) filter (where status = 'dead') as dead from upd")) {
                ps.setInt(1, limit);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return new Reaped(rs.getInt("requeued"), rs.getInt("dead"));
                }
            }
        });
    }

    /**
     * Puts jobs back on the queue without counting the attempt, for a graceful shutdown that
     * could not finish them in time.
     */
    public int release(Collection<UUID> leaseTokens) throws SQLException {
        if (leaseTokens.isEmpty()) return 0;
        String tokens = pgArray(leaseTokens.stream().map(UUID::toString).toList());
        return pool.inTransaction(tx -> {
            int n;
            try (PreparedStatement ps = tx.prepareStatement(
                    "update jobs set status = 'queued', attempts = greatest(attempts - 1, 0), run_at = now(),"
                            + " lease_token = null, lease_expires_at = null, updated_at = now()"
                            + " where status = 'running' and lease_token = any(?::uuid[])")) {
                ps.setString(1, tokens);
                n = ps.executeUpdate();
            }
            try (PreparedStatement ps = tx.prepareStatement(
                    "update job_attempts set outcome = 'released', finished_at = now()"
                            + " where lease_token = any(?::uuid[]) and outcome is null")) {
                ps.setString(1, tokens);
                ps.executeUpdate();
            }
            return n;
        });
    }

    // ------------------------------------------------------------------ queries & admin

    public Optional<Job> get(long id) throws SQLException {
        return pool.withConnection(c -> {
            try (PreparedStatement ps = c.prepareStatement("select " + Job.COLUMNS + " from jobs where id = ?")) {
                ps.setLong(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? Optional.of(Job.from(rs)) : Optional.empty();
                }
            }
        });
    }

    /** Lists jobs, newest first. Null filters match everything. */
    public List<Job> list(JobStatus status, String queue, int limit) throws SQLException {
        return pool.withConnection(c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "select " + Job.COLUMNS + " from jobs"
                            + " where (?::text is null or status = ?) and (?::text is null or queue = ?)"
                            + " order by id desc limit ?")) {
                String s = status == null ? null : status.db();
                ps.setString(1, s);
                ps.setString(2, s);
                ps.setString(3, queue);
                ps.setString(4, queue);
                ps.setInt(5, limit);
                List<Job> out = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) out.add(Job.from(rs));
                }
                return out;
            }
        });
    }

    /** Moves a dead-lettered job back to the queue with a fresh set of attempts. */
    public Optional<Job> retryDead(long id) throws SQLException {
        return pool.withConnection(c -> {
            try (PreparedStatement ps = c.prepareStatement(
                    "update jobs set status = 'queued', attempts = 0, run_at = now(), completed_at = null, updated_at = now()"
                            + " where id = ? and status = 'dead' returning " + Job.COLUMNS)) {
                ps.setLong(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? Optional.of(Job.from(rs)) : Optional.empty();
                }
            }
        });
    }

    public QueueStats stats() throws SQLException {
        return pool.withConnection(c -> {
            Map<String, Map<String, Long>> counts = new LinkedHashMap<>();
            try (ResultSet rs = c.createStatement().executeQuery(
                    "select queue, status, count(*) as n from jobs group by queue, status order by queue, status")) {
                while (rs.next()) {
                    counts.computeIfAbsent(rs.getString("queue"), k -> new LinkedHashMap<>()).put(rs.getString("status"), rs.getLong("n"));
                }
            }
            try (ResultSet rs = c.createStatement().executeQuery(
                    "select"
                            + " count(*) filter (where status = 'succeeded' and completed_at > now() - interval '60 seconds') as done_1m,"
                            + " percentile_cont(0.50) within group (order by extract(epoch from completed_at - created_at))"
                            + "   filter (where status = 'succeeded' and completed_at > now() - interval '5 minutes') as p50,"
                            + " percentile_cont(0.95) within group (order by extract(epoch from completed_at - created_at))"
                            + "   filter (where status = 'succeeded' and completed_at > now() - interval '5 minutes') as p95,"
                            + " percentile_cont(0.99) within group (order by extract(epoch from completed_at - created_at))"
                            + "   filter (where status = 'succeeded' and completed_at > now() - interval '5 minutes') as p99,"
                            + " extract(epoch from now() - min(run_at) filter (where status = 'queued' and run_at <= now())) as oldest"
                            + " from jobs")) {
                rs.next();
                return new QueueStats(counts, rs.getLong("done_1m"), nullableDouble(rs, "p50"), nullableDouble(rs, "p95"),
                        nullableDouble(rs, "p99"), nullableDouble(rs, "oldest"));
            }
        });
    }

    // ------------------------------------------------------------------ helpers

    private void recordFenced(Job job, String message) throws SQLException {
        pool.withConnection(c -> {
            markAttempt(c, job.leaseToken(), "fenced", message);
            return null;
        });
    }

    private static void markAttempt(Connection c, UUID token, String outcome, String error) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "update job_attempts set outcome = ?, error = ?, finished_at = now() where lease_token = ?::uuid")) {
            ps.setString(1, outcome);
            ps.setString(2, error);
            ps.setString(3, token.toString());
            ps.executeUpdate();
        }
    }

    private static Double nullableDouble(ResultSet rs, String col) throws SQLException {
        double v = rs.getDouble(col);
        return rs.wasNull() ? null : v;
    }

    static String describe(Throwable t) {
        String m = t.getClass().getSimpleName() + (t.getMessage() == null ? "" : ": " + t.getMessage());
        return m.length() > MAX_ERROR_LENGTH ? m.substring(0, MAX_ERROR_LENGTH) : m;
    }

    private static double seconds(Duration d) {
        return d.toMillis() / 1000.0;
    }

    /** Text form of a Postgres array of simple tokens (validated names or UUIDs, so no quoting needed). */
    static String pgArray(Collection<String> items) {
        return items.stream().collect(Collectors.joining(",", "{", "}"));
    }
}
