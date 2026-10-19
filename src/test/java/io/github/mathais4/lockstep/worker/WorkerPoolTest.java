package io.github.mathais4.lockstep.worker;

import io.github.mathais4.lockstep.TestDb;
import io.github.mathais4.lockstep.core.Job;
import io.github.mathais4.lockstep.core.JobStatus;
import io.github.mathais4.lockstep.core.JobStore;
import io.github.mathais4.lockstep.core.NewJob;
import io.github.mathais4.lockstep.core.Outcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerPoolTest {

    private JobStore store;

    @BeforeEach
    void setUp() throws Exception {
        TestDb.reset();
        store = TestDb.store();
    }

    private static WorkerPool.Options fast(String name, int threads) {
        return WorkerPool.Options.defaults()
                .withName(name)
                .withThreads(threads)
                .withLease(Duration.ofSeconds(3))
                .withPoll(Duration.ofMillis(5), Duration.ofMillis(50))
                .withReapEvery(Duration.ofMillis(200))
                .withShutdownGrace(Duration.ofSeconds(5));
    }

    private static void awaitTerminal(long expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            if (TestDb.count("select count(*) from jobs where status in ('succeeded', 'dead')") >= expected) return;
            Thread.sleep(20);
        }
        throw new AssertionError("jobs did not finish in time");
    }

    @Test
    void processesFlakyJobsAndAppliesEveryCreditExactlyOnce() throws Exception {
        int n = 400;
        for (int i = 0; i < n; i++) {
            store.enqueue(NewJob.of("ledger.credit").withMaxAttempts(20)
                    .withPayload("{\"account\":\"acct-" + (i % 7) + "\",\"amount\":" + (i + 1) + ",\"failRate\":0.3}"));
        }
        try (WorkerPool a = new WorkerPool(store, DemoHandlers.registry(), fast("a", 4)).start();
             WorkerPool b = new WorkerPool(store, DemoHandlers.registry(), fast("b", 4)).start()) {
            awaitTerminal(n);
            assertTrue(a.counters().retried() + b.counters().retried() > 0, "30% failure rate should cause retries");
        }
        assertEquals(n, TestDb.count("select count(*) from jobs where status = 'succeeded'"));
        assertEquals(n, TestDb.count("select count(*) from ledger"));
        assertEquals(n, TestDb.count("select count(distinct job_id) from ledger"));
        assertEquals((long) n * (n + 1) / 2, TestDb.count("select sum(amount) from ledger"), "every credit applied once");
    }

    @Test
    void badPayloadsAndUnknownTypesGoStraightToTheDeadLetterQueue() throws Exception {
        long bad = store.enqueue(NewJob.of("ledger.credit").withPayload("{\"amount\":\"lots\"}")).job().id();
        long unknown = store.enqueue(NewJob.of("no.such.handler")).job().id();
        long permanent = store.enqueue(NewJob.of("fail").withPayload("{\"permanent\":true}")).job().id();
        try (WorkerPool p = new WorkerPool(store, DemoHandlers.registry(), fast("p", 2)).start()) {
            awaitTerminal(3);
        }
        for (long id : List.of(bad, unknown, permanent)) {
            Job j = store.get(id).orElseThrow();
            assertEquals(JobStatus.DEAD, j.status());
            assertEquals(1, j.attempts(), "no retries for permanent failures");
        }
        assertTrue(store.get(unknown).orElseThrow().lastError().contains("no handler registered"));
    }

    @Test
    void aStalledWorkerIsFencedAndTheCreditIsNotDoubled() throws Exception {
        long id = store.enqueue(NewJob.of("ledger.credit").withPayload("{\"account\":\"a\",\"amount\":10}")).job().id();

        // Worker "stall" freezes before committing (think: long GC pause or network partition)
        // and stops heartbeating, so its 1 s lease runs out and the job is handed to "healthy".
        CountDownLatch stalled = new CountDownLatch(1);
        AtomicBoolean release = new AtomicBoolean();
        WorkerPool.Options stallOpts = fast("stall", 1)
                .withLease(Duration.ofSeconds(1))
                .withMaxRuntime(Duration.ofMillis(100))
                .withReapEvery(Duration.ofHours(1))
                .withFaults(job -> {
                    stalled.countDown();
                    while (!release.get()) Thread.sleep(10);
                });
        try (WorkerPool stall = new WorkerPool(store, DemoHandlers.registry(), stallOpts).start()) {
            assertTrue(stalled.await(10, TimeUnit.SECONDS));
            Thread.sleep(1300); // lease (1 s) expires; no heartbeat past maxRuntime
            try (WorkerPool healthy = new WorkerPool(store, DemoHandlers.registry(), fast("healthy", 1)).start()) {
                awaitTerminal(1);
            }
            release.set(true); // the zombie wakes up and tries to commit
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (stall.counters().fenced() == 0 && System.nanoTime() < deadline) Thread.sleep(10);
            assertEquals(1, stall.counters().fenced());
            assertEquals(0, stall.counters().succeeded());
        }
        Job j = store.get(id).orElseThrow();
        assertEquals(JobStatus.SUCCEEDED, j.status());
        assertTrue(j.workerId() == null || j.workerId().startsWith("healthy"), "completed by the healthy worker");
        assertEquals(1, TestDb.count("select count(*) from ledger where job_id = ?", id));
        assertEquals(1, TestDb.count("select count(*) from job_attempts where job_id = ? and outcome = 'fenced'", id));
    }

    @Test
    void longJobsKeepTheirLeaseThroughHeartbeats() throws Exception {
        long id = store.enqueue(NewJob.of("sleep").withPayload("{\"ms\":2500}")).job().id();
        try (WorkerPool p = new WorkerPool(store, DemoHandlers.registry(), fast("p", 1).withLease(Duration.ofMillis(900))).start()) {
            awaitTerminal(1);
            assertEquals(1, p.counters().succeeded());
        }
        assertEquals(1, store.get(id).orElseThrow().attempts(), "never reaped despite running for 3 leases");
    }

    @Test
    void gracefulShutdownHandsUnfinishedJobsBack() throws Exception {
        long id = store.enqueue(NewJob.of("sleep").withPayload("{\"ms\":60000}")).job().id();
        WorkerPool p = new WorkerPool(store, DemoHandlers.registry(), fast("p", 1).withShutdownGrace(Duration.ofMillis(300))).start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (p.inFlightCount() == 0 && System.nanoTime() < deadline) Thread.sleep(10);
        p.close();
        assertEquals(1, p.counters().released());
        Job j = store.get(id).orElseThrow();
        assertEquals(JobStatus.QUEUED, j.status());
        assertEquals(0, j.attempts(), "an interrupted shutdown isn't the job's fault");
    }

    @Test
    void customHandlersPlugIn() throws Exception {
        HandlerRegistry reg = new HandlerRegistry().register("greet", job -> Outcome.result("{\"hello\":\"world\"}"));
        long id = store.enqueue(NewJob.of("greet")).job().id();
        try (WorkerPool p = new WorkerPool(store, reg, fast("p", 1)).start()) {
            awaitTerminal(1);
        }
        assertEquals("{\"hello\": \"world\"}", store.get(id).orElseThrow().result());
    }
}
