package io.github.mathais4.lockstep;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConfigTest {

    @Test
    void acceptsPostgresStyleUrls() {
        Config c = Config.fromEnv(Map.of("DATABASE_URL", "postgres://app:p%40ss@db.internal:6543/jobs?sslmode=require"));
        assertEquals("jdbc:postgresql://db.internal:6543/jobs?sslmode=require", c.jdbcUrl());
        assertEquals("app", c.dbUser());
        assertEquals("p@ss", c.dbPassword());
    }

    @Test
    void defaultsAndOverrides() {
        Config d = Config.fromEnv(Map.of());
        assertEquals("jdbc:postgresql://localhost:5432/lockstep", d.jdbcUrl());
        assertEquals(List.of("default"), d.queues());
        Config c = Config.fromEnv(Map.of("WORKER_THREADS", "8", "QUEUES", "emails, reports", "LEASE_SECONDS", "10"));
        assertEquals(8, c.workerThreads());
        assertEquals(12, c.dbPoolSize());
        assertEquals(List.of("emails", "reports"), c.queues());
        assertEquals(Duration.ofSeconds(10), c.lease());
        assertThrows(IllegalArgumentException.class, () -> Config.fromEnv(Map.of("PORT", "eighty")));
    }
}
