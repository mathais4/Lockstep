package io.github.mathais4.lockstep.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * A small bounded JDBC connection pool.
 *
 * <p>Lockstep's database work is a handful of short statements, so a simple pool is enough:
 * a semaphore caps open connections, idle ones are reused LIFO (warmest first), and any
 * connection that fails with a connection-class SQLSTATE (08xxx) is discarded instead of reused.
 */
public final class ConnectionPool implements AutoCloseable {

    @FunctionalInterface
    public interface SqlFunction<T> {
        T apply(Connection c) throws SQLException;
    }

    private final String url;
    private final Properties props;
    private final Semaphore permits;
    private final Deque<Connection> idle = new ArrayDeque<>();
    private final long acquireTimeoutMs;
    private volatile boolean closed;

    public ConnectionPool(String url, String user, String password, int maxSize) {
        this(url, user, password, maxSize, 10_000);
    }

    public ConnectionPool(String url, String user, String password, int maxSize, long acquireTimeoutMs) {
        if (maxSize < 1) throw new IllegalArgumentException("maxSize must be >= 1");
        this.url = url;
        this.props = new Properties();
        if (user != null) props.setProperty("user", user);
        if (password != null) props.setProperty("password", password);
        this.permits = new Semaphore(maxSize, true);
        this.acquireTimeoutMs = acquireTimeoutMs;
    }

    /** Runs {@code fn} on a pooled connection in auto-commit mode. */
    public <T> T withConnection(SqlFunction<T> fn) throws SQLException {
        Connection c = acquire();
        boolean broken = false;
        try {
            return fn.apply(c);
        } catch (SQLException e) {
            broken = isConnectionError(e);
            throw e;
        } finally {
            release(c, broken);
        }
    }

    /** Runs {@code fn} inside one transaction: commits if it returns, rolls back if it throws. */
    public <T> T inTransaction(SqlFunction<T> fn) throws SQLException {
        Connection c = acquire();
        boolean broken = false;
        try {
            c.setAutoCommit(false);
            try {
                T out = fn.apply(c);
                c.commit();
                return out;
            } catch (SQLException | RuntimeException e) {
                try {
                    c.rollback();
                } catch (SQLException re) {
                    broken = true;
                }
                throw e;
            } finally {
                if (!broken) c.setAutoCommit(true);
            }
        } catch (SQLException e) {
            broken = broken || isConnectionError(e);
            throw e;
        } finally {
            release(c, broken);
        }
    }

    private Connection acquire() throws SQLException {
        if (closed) throw new SQLException("pool is closed", "08003");
        try {
            if (!permits.tryAcquire(acquireTimeoutMs, TimeUnit.MILLISECONDS)) {
                throw new SQLException("timed out after " + acquireTimeoutMs + " ms waiting for a database connection", "08001");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted waiting for a database connection", "08001", e);
        }
        try {
            Connection c;
            synchronized (idle) {
                c = idle.pollFirst();
            }
            if (c != null && !c.isClosed()) return c;
            return DriverManager.getConnection(url, props);
        } catch (SQLException | RuntimeException e) {
            permits.release();
            throw e;
        }
    }

    private void release(Connection c, boolean broken) {
        try {
            if (broken || closed) {
                closeQuietly(c);
            } else {
                synchronized (idle) {
                    idle.addFirst(c);
                }
            }
        } finally {
            permits.release();
        }
    }

    static boolean isConnectionError(SQLException e) {
        String state = e.getSQLState();
        return state != null && state.startsWith("08");
    }

    private static void closeQuietly(Connection c) {
        try {
            c.close();
        } catch (SQLException ignored) {
            // nothing useful to do
        }
    }

    @Override
    public void close() {
        closed = true;
        synchronized (idle) {
            idle.forEach(ConnectionPool::closeQuietly);
            idle.clear();
        }
    }
}
