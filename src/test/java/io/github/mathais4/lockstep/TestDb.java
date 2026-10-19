package io.github.mathais4.lockstep;

import io.github.mathais4.lockstep.core.JobStore;
import io.github.mathais4.lockstep.core.RetryPolicy;
import io.github.mathais4.lockstep.db.ConnectionPool;
import io.github.mathais4.lockstep.db.Migrator;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;

/**
 * Shared database for tests: PostgreSQL from {@code TEST_DATABASE_URL} (default: the
 * docker-compose database). Each test calls {@link #reset()} for a clean slate.
 */
public final class TestDb {
    private static ConnectionPool pool;

    private TestDb() {}

    public static synchronized ConnectionPool pool() throws SQLException {
        if (pool == null) {
            Map<String, String> env = new java.util.HashMap<>(System.getenv());
            env.put("DATABASE_URL", env.getOrDefault("TEST_DATABASE_URL", "jdbc:postgresql://localhost:5432/lockstep"));
            Config cfg = Config.fromEnv(env);
            pool = new ConnectionPool(cfg.jdbcUrl(), cfg.dbUser(), cfg.dbPassword(), 24);
            Migrator.migrate(pool);
        }
        return pool;
    }

    /** A store whose retries have no delay, so tests don't wait on backoff. */
    public static JobStore store() throws SQLException {
        return new JobStore(pool(), new RetryPolicy(Duration.ZERO, Duration.ZERO, false));
    }

    public static void reset() throws SQLException {
        pool().withConnection(c -> c.createStatement().execute("truncate jobs, job_attempts, ledger restart identity cascade"));
    }

    public static long count(String sql, Object... params) throws SQLException {
        return pool().withConnection(c -> {
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                for (int i = 0; i < params.length; i++) ps.setObject(i + 1, params[i]);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getLong(1);
                }
            }
        });
    }

    /** Makes every running job's lease expire now, as if its worker had died. */
    public static void expireLeases() throws SQLException {
        pool().withConnection(c -> c.createStatement().executeUpdate(
                "update jobs set lease_expires_at = now() - interval '1 second' where status = 'running'"));
    }
}
