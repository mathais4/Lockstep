package io.github.mathais4.lockstep.worker;

import io.github.mathais4.lockstep.core.Job;
import io.github.mathais4.lockstep.core.JobHandler;
import io.github.mathais4.lockstep.core.JobStore;
import io.github.mathais4.lockstep.core.NonRetryableException;
import io.github.mathais4.lockstep.core.Outcome;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Runs jobs. Each worker thread loops: claim a job, run its handler, record the outcome.
 *
 * <p>Two background tasks keep the system healthy: a heartbeat renews the leases of jobs in
 * flight (but stops renewing a job that has run past {@code maxRuntime}, so a hung handler
 * eventually loses its lease), and a reaper returns jobs whose workers died to the queue.
 *
 * <p>Idle workers back off exponentially (with jitter) between empty polls, so an idle pool
 * costs the database a few cheap queries a second rather than a tight loop.
 */
public final class WorkerPool implements AutoCloseable {

    /** Test hook for simulating faults, e.g. a worker that stalls (GC pause, network partition) before completing. */
    @FunctionalInterface
    public interface Faults {
        Faults NONE = job -> { };

        void beforeComplete(Job job) throws InterruptedException;
    }

    public record Options(
            String name,
            int threads,
            List<String> queues,
            Duration lease,
            Duration heartbeatEvery,
            Duration maxRuntime,
            Duration pollMin,
            Duration pollMax,
            Duration reapEvery,
            Duration shutdownGrace,
            int batchSize,
            Faults faults) {

        public static Options defaults() {
            String host = System.getenv().getOrDefault("HOSTNAME", "local");
            return new Options(host + "-" + ProcessHandle.current().pid(), 4, List.of("default"),
                    Duration.ofSeconds(30), Duration.ofSeconds(10), Duration.ofMinutes(15),
                    Duration.ofMillis(20), Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofSeconds(25), 1, Faults.NONE);
        }

        public Options withName(String v) { return new Options(v, threads, queues, lease, heartbeatEvery, maxRuntime, pollMin, pollMax, reapEvery, shutdownGrace, batchSize, faults); }
        public Options withThreads(int v) { return new Options(name, v, queues, lease, heartbeatEvery, maxRuntime, pollMin, pollMax, reapEvery, shutdownGrace, batchSize, faults); }
        public Options withQueues(List<String> v) { return new Options(name, threads, v, lease, heartbeatEvery, maxRuntime, pollMin, pollMax, reapEvery, shutdownGrace, batchSize, faults); }
        /** Sets the lease and derives the heartbeat interval (a third of it) from it. */
        public Options withLease(Duration v) { return new Options(name, threads, queues, v, v.dividedBy(3), maxRuntime, pollMin, pollMax, reapEvery, shutdownGrace, batchSize, faults); }
        public Options withMaxRuntime(Duration v) { return new Options(name, threads, queues, lease, heartbeatEvery, v, pollMin, pollMax, reapEvery, shutdownGrace, batchSize, faults); }
        public Options withPoll(Duration min, Duration max) { return new Options(name, threads, queues, lease, heartbeatEvery, maxRuntime, min, max, reapEvery, shutdownGrace, batchSize, faults); }
        public Options withReapEvery(Duration v) { return new Options(name, threads, queues, lease, heartbeatEvery, maxRuntime, pollMin, pollMax, v, shutdownGrace, batchSize, faults); }
        public Options withShutdownGrace(Duration v) { return new Options(name, threads, queues, lease, heartbeatEvery, maxRuntime, pollMin, pollMax, reapEvery, v, batchSize, faults); }
        public Options withBatchSize(int v) { return new Options(name, threads, queues, lease, heartbeatEvery, maxRuntime, pollMin, pollMax, reapEvery, shutdownGrace, v, faults); }
        public Options withFaults(Faults v) { return new Options(name, threads, queues, lease, heartbeatEvery, maxRuntime, pollMin, pollMax, reapEvery, shutdownGrace, batchSize, v); }
    }

    public record Counters(long claimed, long succeeded, long retried, long dead, long fenced, long released, long reaped) {}

    private record InFlight(Job job, long startedNanos) {}

    private static final Logger LOG = System.getLogger(WorkerPool.class.getName());

    private final JobStore store;
    private final HandlerRegistry handlers;
    private final Options opts;
    private final Map<UUID, InFlight> inFlight = new ConcurrentHashMap<>();
    private final List<Thread> threads = new ArrayList<>();
    private final ScheduledExecutorService background = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "lockstep-background");
        t.setDaemon(true);
        return t;
    });
    private final LongAdder claimed = new LongAdder();
    private final LongAdder succeeded = new LongAdder();
    private final LongAdder retried = new LongAdder();
    private final LongAdder dead = new LongAdder();
    private final LongAdder fenced = new LongAdder();
    private final LongAdder released = new LongAdder();
    private final LongAdder reaped = new LongAdder();
    private volatile boolean running;

    public WorkerPool(JobStore store, HandlerRegistry handlers, Options opts) {
        if (opts.threads() < 1) throw new IllegalArgumentException("threads must be >= 1");
        if (opts.heartbeatEvery().compareTo(opts.lease()) >= 0) throw new IllegalArgumentException("heartbeat must be shorter than the lease");
        this.store = store;
        this.handlers = handlers;
        this.opts = opts;
    }

    public synchronized WorkerPool start() {
        if (running) return this;
        running = true;
        for (int i = 0; i < opts.threads(); i++) {
            String workerId = opts.name() + "/" + i;
            threads.add(Thread.ofPlatform().name("lockstep-" + workerId).start(() -> loop(workerId)));
        }
        long hb = opts.heartbeatEvery().toMillis();
        background.scheduleWithFixedDelay(this::heartbeat, hb, hb, TimeUnit.MILLISECONDS);
        long reap = opts.reapEvery().toMillis();
        background.scheduleWithFixedDelay(this::reap, reap / 2, reap, TimeUnit.MILLISECONDS);
        LOG.log(Level.INFO, "worker pool {0} started: {1} threads on {2}", opts.name(), opts.threads(), opts.queues());
        return this;
    }

    public Counters counters() {
        return new Counters(claimed.sum(), succeeded.sum(), retried.sum(), dead.sum(), fenced.sum(), released.sum(), reaped.sum());
    }

    public int inFlightCount() {
        return inFlight.size();
    }

    // ------------------------------------------------------------------ worker loop

    private void loop(String workerId) {
        long idleMs = opts.pollMin().toMillis();
        while (running) {
            List<Job> jobs;
            try {
                jobs = store.claim(opts.queues(), workerId, opts.batchSize(), opts.lease());
            } catch (SQLException e) {
                LOG.log(Level.WARNING, "claim failed: {0}", e.getMessage());
                pause(opts.pollMax().toMillis());
                continue;
            }
            if (jobs.isEmpty()) {
                // Jitter keeps a pool of idle workers from polling in lockstep (ironically).
                pause(idleMs / 2 + ThreadLocalRandom.current().nextLong(idleMs / 2 + 1));
                idleMs = Math.min(idleMs * 2, opts.pollMax().toMillis());
                continue;
            }
            idleMs = opts.pollMin().toMillis();
            claimed.add(jobs.size());
            for (Job job : jobs) run(job);
        }
    }

    private void run(Job job) {
        inFlight.put(job.leaseToken(), new InFlight(job, System.nanoTime()));
        try {
            JobHandler handler = handlers.find(job.type()).orElse(null);
            if (handler == null) {
                record(job, store.fail(job, new NonRetryableException("no handler registered for type " + job.type()), false));
                return;
            }
            Outcome outcome;
            try {
                outcome = handler.handle(job);
            } catch (NonRetryableException e) {
                record(job, store.fail(job, e, false));
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                record(job, store.fail(job, e, true));
                return;
            } catch (Exception | StackOverflowError e) {
                record(job, store.fail(job, e, true));
                return;
            }
            opts.faults().beforeComplete(job);
            boolean done;
            try {
                done = store.complete(job, outcome);
            } catch (SQLException e) {
                // The effect or the commit failed: the transaction rolled back, so nothing was applied. Retry.
                record(job, store.fail(job, e, true));
                return;
            }
            if (done) succeeded.increment();
            else fenced.increment();
        } catch (SQLException e) {
            // Couldn't record the outcome. The lease will lapse and the reaper will retry the job.
            LOG.log(Level.WARNING, "could not record outcome of job {0}: {1}", job.id(), e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            inFlight.remove(job.leaseToken());
        }
    }

    private void record(Job job, JobStore.FailResult r) {
        switch (r) {
            case RETRY_SCHEDULED -> retried.increment();
            case DEAD -> dead.increment();
            case FENCED -> fenced.increment();
        }
    }

    // ------------------------------------------------------------------ background

    private void heartbeat() {
        long now = System.nanoTime();
        long max = opts.maxRuntime().toNanos();
        List<UUID> renew = new ArrayList<>();
        for (Map.Entry<UUID, InFlight> e : inFlight.entrySet()) {
            if (now - e.getValue().startedNanos() < max) renew.add(e.getKey());
        }
        try {
            store.heartbeat(renew, opts.lease());
        } catch (SQLException e) {
            LOG.log(Level.WARNING, "heartbeat failed: {0}", e.getMessage());
        }
    }

    private void reap() {
        try {
            JobStore.Reaped r;
            do {
                r = store.reapExpired(500);
                reaped.add(r.requeued() + r.dead());
                if (r.requeued() + r.dead() > 0) {
                    LOG.log(Level.INFO, "reaper: {0} jobs requeued, {1} dead-lettered after lease expiry", r.requeued(), r.dead());
                }
            } while (r.requeued() + r.dead() == 500);
        } catch (SQLException e) {
            LOG.log(Level.WARNING, "reaper failed: {0}", e.getMessage());
        }
    }

    private static void pause(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ------------------------------------------------------------------ shutdown

    /**
     * Graceful shutdown: stop claiming, let jobs in flight finish within the grace period,
     * then hand any still running back to the queue (without charging them an attempt).
     */
    @Override
    public void close() {
        synchronized (this) {
            if (!running) return;
            running = false;
        }
        long deadline = System.nanoTime() + opts.shutdownGrace().toNanos();
        for (Thread t : threads) {
            long left = deadline - System.nanoTime();
            if (left > 0) {
                try {
                    t.join(Math.max(1, left / 1_000_000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        if (!inFlight.isEmpty()) {
            List<UUID> tokens = new ArrayList<>(inFlight.keySet());
            try {
                int n = store.release(tokens);
                released.add(n);
                LOG.log(Level.INFO, "shutdown: released {0} unfinished jobs back to the queue", n);
            } catch (SQLException e) {
                LOG.log(Level.WARNING, "could not release jobs on shutdown (their leases will expire): {0}", e.getMessage());
            }
            threads.forEach(Thread::interrupt);
        }
        background.shutdownNow();
        LOG.log(Level.INFO, "worker pool {0} stopped: {1}", opts.name(), counters());
    }
}
