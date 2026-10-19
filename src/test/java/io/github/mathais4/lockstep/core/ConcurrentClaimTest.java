package io.github.mathais4.lockstep.core;

import io.github.mathais4.lockstep.TestDb;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Many workers racing for the same rows: every job must be claimed exactly once. */
class ConcurrentClaimTest {

    private JobStore store;

    @BeforeEach
    void setUp() throws Exception {
        TestDb.reset();
        store = TestDb.store();
    }

    @Test
    void sixteenRacingWorkersNeverClaimTheSameJob() throws Exception {
        int jobs = 3000;
        TestDb.pool().withConnection(c -> c.createStatement().execute(
                "insert into jobs (type) select 'sleep' from generate_series(1, " + jobs + ")"));

        int workers = 16;
        Map<Long, String> claimedBy = new ConcurrentHashMap<>();
        AtomicInteger duplicates = new AtomicInteger();
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        List<Future<Integer>> results = new ArrayList<>();
        for (int w = 0; w < workers; w++) {
            String id = "w" + w;
            results.add(pool.submit(() -> {
                go.await();
                int mine = 0;
                for (;;) {
                    // Batches of 1–4 so workers keep colliding on the head of the queue.
                    List<Job> got = store.claim(List.of("default"), id, 1 + (mine % 4), Duration.ofSeconds(30));
                    if (got.isEmpty()) return mine;
                    for (Job j : got) {
                        if (claimedBy.putIfAbsent(j.id(), id) != null) duplicates.incrementAndGet();
                        mine++;
                    }
                }
            }));
        }
        go.countDown();
        int total = 0;
        int busiest = 0;
        for (Future<Integer> f : results) {
            int n = f.get();
            total += n;
            busiest = Math.max(busiest, n);
        }
        pool.shutdown();

        assertEquals(0, duplicates.get(), "a job was claimed twice");
        assertEquals(jobs, total);
        assertEquals(jobs, claimedBy.size());
        assertTrue(busiest < jobs, "work should be spread across workers, not serialised on one");
        assertEquals(jobs, TestDb.count("select count(*) from jobs where status = 'running' and attempts = 1"));
        assertEquals(jobs, TestDb.count("select count(distinct job_id) from job_attempts"));
    }

    @Test
    void concurrentDuplicateSubmissionsCreateOneJob() throws Exception {
        int clients = 12;
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        List<Future<JobStore.EnqueueResult>> fs = new ArrayList<>();
        for (int i = 0; i < clients; i++) {
            fs.add(pool.submit(() -> {
                go.await();
                return store.enqueue(NewJob.of("sleep").withIdempotencyKey("payment-7"));
            }));
        }
        go.countDown();
        int created = 0;
        long id = -1;
        for (Future<JobStore.EnqueueResult> f : fs) {
            JobStore.EnqueueResult r = f.get();
            if (r.created()) created++;
            if (id == -1) id = r.job().id();
            assertEquals(id, r.job().id(), "every client gets the same job");
        }
        pool.shutdown();
        assertEquals(1, created);
        assertEquals(1, TestDb.count("select count(*) from jobs"));
    }
}
