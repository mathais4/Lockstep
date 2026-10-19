package io.github.mathais4.lockstep;

import io.github.mathais4.lockstep.api.ApiServer;
import io.github.mathais4.lockstep.bench.LoadTest;
import io.github.mathais4.lockstep.core.JobStore;
import io.github.mathais4.lockstep.core.RetryPolicy;
import io.github.mathais4.lockstep.db.ConnectionPool;
import io.github.mathais4.lockstep.db.Migrator;
import io.github.mathais4.lockstep.worker.DemoHandlers;
import io.github.mathais4.lockstep.worker.WorkerPool;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Entry point.
 *
 * <pre>
 * lockstep api        REST API only
 * lockstep worker     worker pool only (scale these out)
 * lockstep all        both in one process (local development)
 * lockstep migrate    apply the schema and exit
 * lockstep loadtest   multi-process load + crash test (see LoadTest)
 * </pre>
 */
public final class Main {
    private Main() {}

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "all";
        if (mode.equals("loadtest")) {
            LoadTest.main(Arrays.copyOfRange(args, 1, args.length));
            return;
        }
        if (!List.of("api", "worker", "all", "migrate").contains(mode)) {
            System.err.println("usage: lockstep [api|worker|all|migrate|loadtest]");
            System.exit(2);
        }

        Config cfg = Config.fromEnv();
        ConnectionPool pool = new ConnectionPool(cfg.jdbcUrl(), cfg.dbUser(), cfg.dbPassword(), cfg.dbPoolSize());
        List<String> applied = Migrator.migrate(pool);
        if (!applied.isEmpty()) System.out.println("applied migrations: " + applied);
        if (mode.equals("migrate")) {
            pool.close();
            return;
        }

        JobStore store = new JobStore(pool, RetryPolicy.DEFAULT);
        ApiServer api = mode.equals("worker") ? null : new ApiServer(store, cfg.port()).start();
        WorkerPool workers = mode.equals("api") ? null : new WorkerPool(store, DemoHandlers.registry(),
                WorkerPool.Options.defaults()
                        .withName(System.getenv().getOrDefault("WORKER_NAME", WorkerPool.Options.defaults().name()))
                        .withThreads(cfg.workerThreads())
                        .withQueues(cfg.queues())
                        .withLease(cfg.lease())).start();

        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // SIGTERM (docker stop, Ctrl-C): finish or hand back in-flight jobs, then exit.
            if (workers != null) workers.close();
            if (api != null) api.close();
            pool.close();
            stopped.countDown();
        }, "lockstep-shutdown"));
        stopped.await();
    }
}
