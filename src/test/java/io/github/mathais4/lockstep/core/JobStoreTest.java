package io.github.mathais4.lockstep.core;

import io.github.mathais4.lockstep.TestDb;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobStoreTest {

    private static final List<String> DEFAULT = List.of("default");
    private static final Duration LEASE = Duration.ofSeconds(30);
    private JobStore store;

    @BeforeEach
    void setUp() throws Exception {
        TestDb.reset();
        store = TestDb.store();
    }

    @Test
    void enqueueThenClaimMarksTheJobRunningUnderALease() throws Exception {
        long id = store.enqueue(NewJob.of("sleep").withPayload("{\"ms\":1}")).job().id();
        List<Job> claimed = store.claim(DEFAULT, "w1", 10, LEASE);
        assertEquals(1, claimed.size());
        Job j = claimed.get(0);
        assertEquals(id, j.id());
        assertEquals(JobStatus.RUNNING, j.status());
        assertEquals(1, j.attempts());
        assertEquals("w1", j.workerId());
        assertNotNull(j.leaseToken());
        assertTrue(j.leaseExpiresAt().isAfter(j.createdAt().plusSeconds(25)));
        assertEquals(0, store.claim(DEFAULT, "w2", 10, LEASE).size(), "a running job is not claimable");
        assertEquals(1, TestDb.count("select count(*) from job_attempts where job_id = ?", id));
    }

    @Test
    void idempotencyKeyReturnsTheExistingJob() throws Exception {
        JobStore.EnqueueResult a = store.enqueue(NewJob.of("sleep").withIdempotencyKey("order-42"));
        JobStore.EnqueueResult b = store.enqueue(NewJob.of("sleep").withIdempotencyKey("order-42").withPriority(9));
        assertTrue(a.created());
        assertFalse(b.created());
        assertEquals(a.job().id(), b.job().id());
        assertEquals(0, b.job().priority(), "the original job is returned unchanged");
        // Keys are scoped per queue.
        assertTrue(store.enqueue(NewJob.of("sleep").withQueue("other").withIdempotencyKey("order-42")).created());
        assertEquals(2, TestDb.count("select count(*) from jobs"));
    }

    @Test
    void claimsHighestPriorityFirstThenOldest() throws Exception {
        long low = store.enqueue(NewJob.of("sleep")).job().id();
        long high = store.enqueue(NewJob.of("sleep").withPriority(5)).job().id();
        long low2 = store.enqueue(NewJob.of("sleep")).job().id();
        List<Long> order = store.claim(DEFAULT, "w", 10, LEASE).stream().map(Job::id).toList();
        assertEquals(List.of(high, low, low2), order);
    }

    @Test
    void delayedJobsWaitUntilTheyAreDue() throws Exception {
        store.enqueue(NewJob.of("sleep").withDelaySeconds(60));
        long now = store.enqueue(NewJob.of("sleep")).job().id();
        List<Job> claimed = store.claim(DEFAULT, "w", 10, LEASE);
        assertEquals(List.of(now), claimed.stream().map(Job::id).toList());
    }

    @Test
    void queuesAreIsolated() throws Exception {
        store.enqueue(NewJob.of("sleep").withQueue("emails"));
        assertEquals(0, store.claim(DEFAULT, "w", 10, LEASE).size());
        assertEquals(1, store.claim(List.of("emails", "default"), "w", 10, LEASE).size());
    }

    @Test
    void severalQueuesAreServedInRotationSoNoneStarves() throws Exception {
        for (int i = 0; i < 5; i++) {
            store.enqueue(NewJob.of("sleep").withQueue("busy").withPriority(9));
            store.enqueue(NewJob.of("sleep").withQueue("quiet"));
        }
        List<String> served = new java.util.ArrayList<>();
        for (int i = 0; i < 4; i++) served.add(store.claim(List.of("busy", "quiet"), "w", 1, LEASE).get(0).queue());
        assertEquals(2, served.stream().filter("quiet"::equals).count(), "quiet queue served despite busy's higher priority: " + served);
    }

    @Test
    void completeStoresTheResultAndAppliesTheEffectOnce() throws Exception {
        store.enqueue(NewJob.of("ledger.credit"));
        Job j = store.claim(DEFAULT, "w", 1, LEASE).get(0);
        boolean ok = store.complete(j, Outcome.withEffect("{\"ok\":true}", tx ->
                tx.createStatement().executeUpdate("insert into ledger (job_id, account, amount) values (" + j.id() + ", 'a', 5)")));
        assertTrue(ok);
        Job done = store.get(j.id()).orElseThrow();
        assertEquals(JobStatus.SUCCEEDED, done.status());
        assertEquals("{\"ok\": true}", done.result());
        assertNull(done.leaseToken());
        assertNotNull(done.completedAt());
        assertEquals(1, TestDb.count("select count(*) from ledger where job_id = ?", j.id()));
        assertEquals(1, TestDb.count("select count(*) from job_attempts where job_id = ? and outcome = 'succeeded'", j.id()));
    }

    @Test
    void aFailingEffectRollsBackTheCompletion() throws Exception {
        store.enqueue(NewJob.of("ledger.credit"));
        Job j = store.claim(DEFAULT, "w", 1, LEASE).get(0);
        assertThrows(java.sql.SQLException.class, () -> store.complete(j, Outcome.withEffect(null, tx ->
                tx.createStatement().executeUpdate("insert into ledger (job_id, account, amount) values (" + j.id() + ", null, 5)"))));
        assertEquals(JobStatus.RUNNING, store.get(j.id()).orElseThrow().status(), "still running: nothing committed");
        assertEquals(0, TestDb.count("select count(*) from ledger"));
    }

    @Test
    void failuresRetryUntilAttemptsRunOutThenDeadLetter() throws Exception {
        long id = store.enqueue(NewJob.of("fail").withMaxAttempts(3)).job().id();
        for (int attempt = 1; attempt <= 3; attempt++) {
            Job j = store.claim(DEFAULT, "w", 1, LEASE).get(0);
            assertEquals(attempt, j.attempts());
            JobStore.FailResult r = store.fail(j, new IllegalStateException("boom " + attempt), true);
            assertEquals(attempt < 3 ? JobStore.FailResult.RETRY_SCHEDULED : JobStore.FailResult.DEAD, r);
        }
        Job dead = store.get(id).orElseThrow();
        assertEquals(JobStatus.DEAD, dead.status());
        assertEquals("IllegalStateException: boom 3", dead.lastError());
        assertEquals(0, store.claim(DEFAULT, "w", 1, LEASE).size());
        assertEquals(3, TestDb.count("select count(*) from job_attempts where job_id = ? and outcome = 'failed'", id));
    }

    @Test
    void backoffPushesTheRetryIntoTheFuture() throws Exception {
        JobStore slow = new JobStore(TestDb.pool(), new RetryPolicy(Duration.ofSeconds(30), Duration.ofSeconds(30), false));
        long id = slow.enqueue(NewJob.of("fail")).job().id();
        Job j = slow.claim(DEFAULT, "w", 1, LEASE).get(0);
        slow.fail(j, new RuntimeException("x"), true);
        assertEquals(0, slow.claim(DEFAULT, "w", 1, LEASE).size(), "not due yet");
        assertEquals(1, TestDb.count("select count(*) from jobs where id = ? and run_at > now() + interval '25 seconds'", id));
    }

    @Test
    void nonRetryableFailuresDeadLetterImmediately() throws Exception {
        long id = store.enqueue(NewJob.of("fail").withMaxAttempts(10)).job().id();
        Job j = store.claim(DEFAULT, "w", 1, LEASE).get(0);
        assertEquals(JobStore.FailResult.DEAD, store.fail(j, new NonRetryableException("bad input"), false));
        assertEquals(JobStatus.DEAD, store.get(id).orElseThrow().status());
    }

    @Test
    void retryDeadRequeuesWithFreshAttempts() throws Exception {
        long id = store.enqueue(NewJob.of("fail").withMaxAttempts(1)).job().id();
        store.fail(store.claim(DEFAULT, "w", 1, LEASE).get(0), new RuntimeException(), true);
        Job back = store.retryDead(id).orElseThrow();
        assertEquals(JobStatus.QUEUED, back.status());
        assertEquals(0, back.attempts());
        assertTrue(store.retryDead(id).isEmpty(), "only dead jobs can be retried");
        assertEquals(1, store.claim(DEFAULT, "w", 1, LEASE).get(0).attempts());
    }

    @Test
    void reaperRequeuesJobsWhoseWorkerDied() throws Exception {
        long id = store.enqueue(NewJob.of("sleep")).job().id();
        store.claim(DEFAULT, "dead-worker", 1, LEASE);
        assertEquals(new JobStore.Reaped(0, 0), store.reapExpired(100), "lease still valid");
        TestDb.expireLeases();
        assertEquals(new JobStore.Reaped(1, 0), store.reapExpired(100));
        Job j = store.get(id).orElseThrow();
        assertEquals(JobStatus.QUEUED, j.status());
        assertTrue(j.lastError().contains("dead-worker"));
        assertEquals(1, TestDb.count("select count(*) from job_attempts where job_id = ? and outcome = 'lease_expired'", id));
    }

    @Test
    void reaperDeadLettersAJobThatKeepsKillingWorkers() throws Exception {
        long id = store.enqueue(NewJob.of("sleep").withMaxAttempts(2)).job().id();
        for (int i = 0; i < 2; i++) {
            store.claim(DEFAULT, "w" + i, 1, LEASE);
            TestDb.expireLeases();
            store.reapExpired(100);
        }
        assertEquals(JobStatus.DEAD, store.get(id).orElseThrow().status());
    }

    @Test
    void fencingRejectsAZombieWorkersLateCompletion() throws Exception {
        long id = store.enqueue(NewJob.of("ledger.credit")).job().id();
        Job zombie = store.claim(DEFAULT, "zombie", 1, LEASE).get(0);
        TestDb.expireLeases();
        store.reapExpired(100);
        Job fresh = store.claim(DEFAULT, "fresh", 1, LEASE).get(0);
        assertNotEquals(zombie.leaseToken(), fresh.leaseToken());

        Outcome.SqlEffect credit = tx -> tx.createStatement().executeUpdate("insert into ledger (job_id, account, amount) values (" + id + ", 'a', 1)");
        assertTrue(store.complete(fresh, Outcome.withEffect("{\"by\":\"fresh\"}", credit)));
        assertFalse(store.complete(zombie, Outcome.withEffect("{\"by\":\"zombie\"}", credit)), "zombie must be fenced");
        assertEquals(JobStore.FailResult.FENCED, store.fail(zombie, new RuntimeException("late"), true));

        assertEquals("{\"by\": \"fresh\"}", store.get(id).orElseThrow().result());
        assertEquals(1, TestDb.count("select count(*) from ledger where job_id = ?", id), "effect applied exactly once");
        assertEquals(1, TestDb.count("select count(*) from job_attempts where job_id = ? and outcome = 'fenced'", id));
    }

    @Test
    void heartbeatExtendsOnlyTheGivenLeases() throws Exception {
        store.enqueue(NewJob.of("sleep"));
        store.enqueue(NewJob.of("sleep"));
        List<Job> js = store.claim(DEFAULT, "w", 2, Duration.ofSeconds(1));
        assertEquals(1, store.heartbeat(List.of(js.get(0).leaseToken()), Duration.ofSeconds(120)));
        assertEquals(1, TestDb.count("select count(*) from jobs where lease_expires_at > now() + interval '100 seconds'"));
    }

    @Test
    void releaseRequeuesWithoutChargingAnAttempt() throws Exception {
        long id = store.enqueue(NewJob.of("sleep")).job().id();
        Job j = store.claim(DEFAULT, "w", 1, LEASE).get(0);
        assertEquals(1, store.release(List.of(j.leaseToken())));
        Job back = store.get(id).orElseThrow();
        assertEquals(JobStatus.QUEUED, back.status());
        assertEquals(0, back.attempts());
        assertEquals(1, store.claim(DEFAULT, "w", 1, LEASE).get(0).attempts());
        assertEquals(1, TestDb.count("select count(*) from job_attempts where job_id = ? and outcome = 'released'", id));
    }

    @Test
    void statsCountByQueueAndStatus() throws Exception {
        store.enqueue(NewJob.of("sleep"));
        store.enqueue(NewJob.of("sleep").withQueue("emails"));
        Job j = store.claim(DEFAULT, "w", 1, LEASE).get(0);
        store.complete(j, Outcome.ok());
        JobStore.QueueStats s = store.stats();
        assertEquals(Long.valueOf(1), s.countsByQueue().get("default").get("succeeded"));
        assertEquals(Long.valueOf(1), s.countsByQueue().get("emails").get("queued"));
        assertEquals(1, s.succeededLastMinute());
        assertNotNull(s.latencyP95Seconds());
    }

    @Test
    void rejectsInvalidJobs() {
        assertThrows(IllegalArgumentException.class, () -> NewJob.of("has spaces"));
        assertThrows(IllegalArgumentException.class, () -> NewJob.of("ok").withQueue(""));
        assertThrows(IllegalArgumentException.class, () -> NewJob.of("ok").withMaxAttempts(0));
        assertThrows(IllegalArgumentException.class, () -> NewJob.of("ok").withDelaySeconds(-1));
    }
}
