package io.github.mathais4.lockstep.db;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Applies the SQL files in {@code /db} on the classpath, once each, in version order.
 * A transaction-scoped advisory lock stops two processes starting at once from racing.
 */
public final class Migrator {

    /** Listed explicitly: classpath directories can't be enumerated reliably inside a jar. */
    public static final List<String> MIGRATIONS = List.of("V1__jobs.sql");

    private static final long LOCK_KEY = 0x10C5_7E9L;

    private Migrator() {}

    public static List<String> migrate(ConnectionPool pool) throws SQLException {
        return pool.inTransaction(c -> {
            try (PreparedStatement lock = c.prepareStatement("select pg_advisory_xact_lock(?)")) {
                lock.setLong(1, LOCK_KEY);
                lock.executeQuery().close();
            }
            c.createStatement().execute(
                    "create table if not exists schema_migrations (name text primary key, applied_at timestamptz not null default now())");
            Set<String> done = new HashSet<>();
            try (ResultSet rs = c.createStatement().executeQuery("select name from schema_migrations")) {
                while (rs.next()) done.add(rs.getString(1));
            }
            List<String> applied = new ArrayList<>();
            for (String name : MIGRATIONS) {
                if (done.contains(name)) continue;
                c.createStatement().execute(read("/db/" + name));
                try (PreparedStatement ins = c.prepareStatement("insert into schema_migrations (name) values (?)")) {
                    ins.setString(1, name);
                    ins.executeUpdate();
                }
                applied.add(name);
            }
            return applied;
        });
    }

    private static String read(String resource) throws SQLException {
        try (InputStream in = Migrator.class.getResourceAsStream(resource)) {
            if (in == null) throw new SQLException("migration not found on classpath: " + resource);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new SQLException("could not read " + resource, e);
        }
    }
}
