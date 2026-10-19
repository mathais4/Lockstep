package io.github.mathais4.lockstep.bench;

import io.github.mathais4.lockstep.Config;
import io.github.mathais4.lockstep.core.JobStore;
import io.github.mathais4.lockstep.core.NewJob;
import io.github.mathais4.lockstep.core.RetryPolicy;
import io.github.mathais4.lockstep.db.ConnectionPool;
import io.github.mathais4.lockstep.db.Migrator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/**
 * Load and crash test with real worker processes.
 *
 * <p><b>Drain:</b> fills the queue with N jobs, starts P worker processes, and measures how fast
 * they empty it, for each P in {@code --processes}.
 *
 * <p><b>Chaos:</b> submits jobs at a steady rate (some of them duplicate submissions, some
 * flaky, some permanently broken) while worker processes are killed with SIGKILL mid-run and
 * replaced. Then checks the invariants: every job finished, no duplicate jobs from repeated
 * submissions, and every ledger credit applied exactly once.
 *
 * <pre>
 * lockstep loadtest [--jobs 20000] [--processes 1,2,4] [--threads 4] [--work-ms 2]
 *                   [--chaos-seconds 40] [--rate 300] [--chaos-processes 4] [--kills 3]
 *                   [--fail-rate 0.05] [--dup-rate 0.05] [--poison-rate 0.01] [--lease 3]
 *                   [--only drain|chaos] [--out bench/results.md]
 * </pre>
 */
public final class LoadTest {

    private final Map<String, String> args;
    private final Config cfg;
    private final ConnectionPool pool;
    private final JobStore store;
    private final Path logDir;
    private final List<Process> procs = new ArrayList<>();
    private int procCounter;
    private boolean failed;

    private LoadTest(Map<String, String> args) throws IOException {
        this.args = args;
        this.cfg = Config.fromEnv();
        this.pool = new ConnectionPool(cfg.jdbcUrl(), cfg.dbUser(), cfg.dbPassword(), 8);
        this.store = new JobStore(pool, RetryPolicy.DEFAULT);
        this.logDir = Files.createTempDirectory("lockstep-loadtest");
    }

    public static void main(String[] argv) throws Exception {
        Map<String, String> a = new HashMap<>();
        for (int i = 0; i + 1 < argv.length; i += 2) {
            if (!argv[i].startsWith("--")) throw new IllegalArgumentException("expected --option value, got " + argv[i]);
            a.put(argv[i].substring(2), argv[i + 1]);
        }
        LoadTest t = new LoadTest(a);
        try {
            t.run();
        } finally {
            t.stopAll(true);
            t.pool.close();
        }
        if (t.failed) System.exit(1);
    }

    private void run() throws Exception {
        Migrator.migrate(pool);
        String only = args.getOrDefault("only", "");
        StringBuilder md = new StringBuilder();
        md.append("# Lockstep load test\n\n");
        md.append(String.format("%s · Java %s · %d CPU cores · PostgreSQL on the same machine. Logs: `%s`%n%n",
                LocalDate.now(), System.getProperty("java.version"), Runtime.getRuntime().availableProcessors(), logDir));
        if (!only.equals("chaos")) md.append(drain());
        if (!only.equals("drain")) md.append(chaos());
        System.out.println();
        System.out.println(md);
        String out = args.getOrDefault("out", "bench/results.md");
        Path p = Path.of(out);
        if (p.getParent() != null) Files.createDirectories(p.getParent());
        Files.writeString(p, md.toString());
        System.out.println("wrote " + p.toAbsolutePath());
    }

    // ------------------------------------------------------------------ drain

    private String drain() throws Exception {
        int jobs = intArg("jobs", 20_000);
        int threads = intArg("threads", 4);
        int workMs = intArg("work-ms", 2);
        StringBuilder md = new StringBuilder();
        md.append("## Drain: throughput by worker processes\n\n");
        md.append(String.format("%,d `ledger.credit` jobs queued up front, each doing %d ms of simulated work, then a database write "
                + "in the completion transaction. %d threads per worker process. Timed from first claim to last completion.%n%n", jobs, workMs, threads));
        md.append("| Worker processes | Threads | Jobs/s | Wall time | p95 claim-to-done | Ledger rows | Duplicate effects |\n");
        md.append("|---:|---:|---:|---:|---:|---:|---:|\n");
        for (String ps : args.getOrDefault("processes", "1,2,4").split(",")) {
            int processes = Integer.parseInt(ps.trim());
            reset();
            exec("insert into jobs (type, payload, max_attempts) "
                    + "select 'ledger.credit', jsonb_build_object('account', 'acct-' || (g % 50), 'amount', 1, 'workMs', " + workMs + "), 8 "
                    + "from generate_series(1, " + jobs + ") g");
            System.out.printf("drain: %d processes × %d threads, %,d jobs%n", processes, threads, jobs);
            for (int i = 0; i < processes; i++) spawn(threads, 30);
            waitForTerminal(jobs, Duration.ofMinutes(10));
            stopAll(false);
            Map<String, Double> r = row("select count(*) as n,"
                    + " extract(epoch from max(completed_at) - min(first_started_at)) as wall,"
                    + " percentile_cont(0.95) within group (order by extract(epoch from completed_at - first_started_at)) as p95"
                    + " from jobs where status = 'succeeded'");
            Map<String, Double> l = ledgerCheck();
            double rate = r.get("n") / r.get("wall");
            md.append(String.format("| %d | %d | %,.0f | %.1f s | %.0f ms | %,.0f | %.0f |%n",
                    processes, processes * threads, rate, r.get("wall"), r.get("p95") * 1000, l.get("rows"), l.get("duplicates")));
            System.out.printf("  %,.0f jobs/s%n", rate);
        }
        md.append("\n");
        return md.toString();
    }

    // ------------------------------------------------------------------ chaos

    private String chaos() throws Exception {
        int seconds = intArg("chaos-seconds", 40);
        int rate = intArg("rate", 300);
        int processes = intArg("chaos-processes", 4);
        int threads = intArg("threads", 4);
        int kills = intArg("kills", 3);
        int leaseSeconds = intArg("lease", 3);
        int workMs = intArg("work-ms", 2);
        double failRate = dblArg("fail-rate", 0.05);
        double dupRate = dblArg("dup-rate", 0.05);
        double poisonRate = dblArg("poison-rate", 0.01);
        Random rnd = new Random(42);

        reset();
        System.out.printf("chaos: %d processes × %d threads, %d jobs/s for %d s, %d SIGKILLs, lease %d s%n",
                processes, threads, rate, seconds, kills, leaseSeconds);
        for (int i = 0; i < processes; i++) spawn(threads, leaseSeconds);
        Thread.sleep(1500); // let JVMs start so the first jobs aren't measured against JVM startup

        int submitted = 0;
        int duplicates = 0;
        int unique = 0;
        int poison = 0;
        int killsDone = 0;
        long start = System.nanoTime();
        long total = (long) seconds * rate;
        for (long i = 0; i < total; i++) {
            long due = start + (long) (i * 1e9 / rate);
            long wait = due - System.nanoTime();
            if (wait > 0) Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));

            // Kill a worker with SIGKILL at evenly spaced points, then start a replacement.
            if (killsDone < kills && i >= total * (killsDone + 1) / (kills + 1)) {
                Process victim = procs.remove(rnd.nextInt(procs.size()));
                victim.destroyForcibly();
                killsDone++;
                System.out.printf("  t=%.1fs SIGKILL worker pid %d, starting a replacement%n", (System.nanoTime() - start) / 1e9, victim.pid());
                spawn(threads, leaseSeconds);
            }

            NewJob job;
            if (unique > 0 && rnd.nextDouble() < dupRate) {
                // A client retrying a request it thinks failed: same key, must not create a second job.
                job = ledgerJob(rnd.nextInt(unique), workMs, failRate);
                duplicates++;
            } else if (rnd.nextDouble() < poisonRate) {
                job = NewJob.of("fail").withPayload("{\"permanent\":true,\"message\":\"malformed input\"}")
                        .withIdempotencyKey("poison-" + i).withMaxAttempts(8);
                poison++;
            } else {
                job = ledgerJob(unique++, workMs, failRate);
            }
            store.enqueue(job);
            submitted++;
        }
        double submitSeconds = (System.nanoTime() - start) / 1e9;
        int expectedJobs = unique + poison;
        waitForTerminal(expectedJobs, Duration.ofMinutes(5));
        stopAll(false);

        Map<String, Double> j = row("select count(*) as jobs,"
                + " count(*) filter (where status = 'succeeded') as succeeded,"
                + " count(*) filter (where status = 'dead') as dead,"
                + " count(*) filter (where status not in ('succeeded', 'dead')) as unfinished,"
                + " percentile_cont(0.50) within group (order by extract(epoch from completed_at - created_at)) filter (where status = 'succeeded') as p50,"
                + " percentile_cont(0.95) within group (order by extract(epoch from completed_at - created_at)) filter (where status = 'succeeded') as p95,"
                + " percentile_cont(0.99) within group (order by extract(epoch from completed_at - created_at)) filter (where status = 'succeeded') as p99,"
                + " max(extract(epoch from completed_at - created_at)) filter (where status = 'succeeded') as pmax,"
                + " percentile_cont(0.95) within group (order by extract(epoch from completed_at - created_at)) filter (where status = 'succeeded' and attempts = 1) as p95_first,"
                + " percentile_cont(0.99) within group (order by extract(epoch from completed_at - created_at)) filter (where status = 'succeeded' and attempts = 1) as p99_first,"
                + " count(*) filter (where status = 'dead' and type = 'fail') as poison_dead,"
                + " count(*) filter (where attempts > 1) as retried"
                + " from jobs");
        Map<String, Double> a = row("select"
                + " count(*) filter (where outcome = 'lease_expired') as lease_expired,"
                + " count(*) filter (where outcome = 'fenced') as fenced,"
                + " count(*) filter (where outcome = 'failed') as failed,"
                + " count(*) filter (where outcome is null) as open"
                + " from job_attempts");
        Map<String, Double> l = ledgerCheck();

        boolean ok = j.get("unfinished") == 0 && j.get("jobs") == expectedJobs && l.get("duplicates") == 0
                && l.get("missing") == 0 && l.get("on_dead") == 0 && j.get("poison_dead") == poison;

        StringBuilder md = new StringBuilder();
        md.append("## Chaos: steady load with workers killed mid-run\n\n");
        md.append(String.format("%d worker processes × %d threads. %d jobs/s submitted for %d s through `JobStore.enqueue` "
                        + "(%,d submissions in %.1f s). %.0f%% of submissions repeat an earlier idempotency key, %.0f%% of ledger jobs "
                        + "fail transiently on each attempt, %.0f%% of jobs are permanently broken. %d workers killed with SIGKILL "
                        + "at evenly spaced points and replaced. Lease %d s.%n%n",
                processes, threads, rate, seconds, submitted, submitSeconds, dupRate * 100, failRate * 100, poisonRate * 100, kills, leaseSeconds));
        md.append("| Measure | Result |\n|---|---|\n");
        md.append(String.format("| Submissions (incl. duplicates) | %,d (%,d duplicates) |%n", submitted, duplicates));
        md.append(String.format("| Jobs created | %,.0f (expected %,d: duplicates collapsed) |%n", j.get("jobs"), expectedJobs));
        md.append(String.format("| Succeeded | %,.0f |%n", j.get("succeeded")));
        md.append(String.format("| Dead-lettered | %,.0f (all %d poison jobs, on their first attempt) |%n", j.get("dead"), poison));
        md.append(String.format("| Unfinished | %,.0f |%n", j.get("unfinished")));
        md.append(String.format("| Jobs that needed a retry | %,.0f |%n", j.get("retried")));
        md.append(String.format("| Attempts lost to killed workers, recovered by the reaper | %,.0f |%n", a.get("lease_expired")));
        md.append(String.format("| Late completions rejected by fencing | %,.0f |%n", a.get("fenced")));
        md.append(String.format("| Latency, enqueue to done, all jobs: p50 / p95 / p99 | %.0f / %.0f / %.0f ms |%n", j.get("p50") * 1000, j.get("p95") * 1000, j.get("p99") * 1000));
        md.append(String.format("| Latency, jobs that succeeded first time: p95 / p99 | %.0f / %.0f ms (the rest include retry backoff) |%n", j.get("p95_first") * 1000, j.get("p99_first") * 1000));
        md.append(String.format("| Latency, worst (a job caught in a crash waits out the lease) | %.2f s |%n", j.get("pmax")));
        md.append(String.format("| Ledger credits applied twice | **%.0f** |%n", l.get("duplicates")));
        md.append(String.format("| Succeeded jobs with no ledger credit | **%.0f** |%n", l.get("missing")));
        md.append(String.format("| Ledger credits for dead-lettered jobs | **%.0f** |%n", l.get("on_dead")));
        md.append(String.format("%n**Invariants: %s**%n", ok ? "all held" : "VIOLATED"));
        System.out.println(ok ? "chaos: invariants held" : "chaos: INVARIANTS VIOLATED");
        if (!ok) failed = true;
        return md.toString();
    }

    private static NewJob ledgerJob(int n, int workMs, double failRate) {
        return NewJob.of("ledger.credit")
                .withPayload(String.format(Locale.ROOT, "{\"account\":\"acct-%d\",\"amount\":1,\"workMs\":%d,\"failRate\":%s}", n % 50, workMs, failRate))
                .withIdempotencyKey("credit-" + n)
                .withMaxAttempts(8);
    }

    // ------------------------------------------------------------------ processes

    private void spawn(int threads, int leaseSeconds) throws IOException {
        String java = ProcessHandle.current().info().command().orElse("java");
        int n = ++procCounter;
        ProcessBuilder pb = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), "io.github.mathais4.lockstep.Main", "worker");
        Map<String, String> env = pb.environment();
        env.put("DATABASE_URL", cfg.jdbcUrl());
        env.put("DB_USER", cfg.dbUser());
        env.put("DB_PASSWORD", cfg.dbPassword());
        env.put("WORKER_THREADS", String.valueOf(threads));
        env.put("LEASE_SECONDS", String.valueOf(leaseSeconds));
        env.put("WORKER_NAME", "w" + n);
        File log = logDir.resolve("worker-" + n + ".log").toFile();
        pb.redirectErrorStream(true).redirectOutput(log);
        procs.add(pb.start());
    }

    private void stopAll(boolean force) {
        for (Process p : procs) {
            if (force) p.destroyForcibly();
            else p.destroy(); // SIGTERM: graceful shutdown
        }
        for (Process p : procs) {
            try {
                if (!p.waitFor(30, java.util.concurrent.TimeUnit.SECONDS)) p.destroyForcibly().waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        procs.clear();
    }

    private void waitForTerminal(int expected, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        double last = -1;
        while (System.nanoTime() < deadline) {
            Map<String, Double> r = row("select count(*) filter (where status in ('succeeded', 'dead')) as done, count(*) as total from jobs");
            if (r.get("done") >= expected && r.get("done").equals(r.get("total"))) return;
            if (r.get("done") != last) {
                System.out.printf("  %,.0f / %,d done%n", r.get("done"), expected);
                last = r.get("done");
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("timed out waiting for jobs to finish");
    }

    // ------------------------------------------------------------------ sql helpers

    private Map<String, Double> ledgerCheck() throws SQLException {
        return row("select"
                + " (select count(*) from ledger) as rows,"
                + " (select count(*) from (select job_id from ledger group by job_id having count(*) > 1) d) as duplicates,"
                + " (select count(*) from jobs j where j.type = 'ledger.credit' and j.status = 'succeeded'"
                + "    and not exists (select 1 from ledger l where l.job_id = j.id)) as missing,"
                + " (select count(*) from ledger l join jobs j on j.id = l.job_id where j.status = 'dead') as on_dead");
    }

    private void reset() throws SQLException {
        exec("truncate jobs, job_attempts, ledger restart identity cascade");
    }

    private void exec(String sql) throws SQLException {
        pool.withConnection(c -> c.createStatement().execute(sql));
    }

    private Map<String, Double> row(String sql) throws SQLException {
        return pool.withConnection(c -> {
            try (PreparedStatement ps = c.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
                rs.next();
                ResultSetMetaData md = rs.getMetaData();
                Map<String, Double> m = new LinkedHashMap<>();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    double v = rs.getDouble(i);
                    m.put(md.getColumnLabel(i), rs.wasNull() ? Double.NaN : v);
                }
                return m;
            }
        });
    }

    private int intArg(String k, int d) {
        return args.containsKey(k) ? Integer.parseInt(args.get(k)) : d;
    }

    private double dblArg(String k, double d) {
        return args.containsKey(k) ? Double.parseDouble(args.get(k)) : d;
    }
}
