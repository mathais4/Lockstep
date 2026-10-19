package io.github.mathais4.lockstep;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Settings from environment variables, with defaults that match docker-compose.yml. */
public record Config(
        String jdbcUrl,
        String dbUser,
        String dbPassword,
        int port,
        int workerThreads,
        List<String> queues,
        Duration lease,
        int dbPoolSize) {

    public static Config fromEnv() {
        return fromEnv(System.getenv());
    }

    public static Config fromEnv(Map<String, String> env) {
        String url = env.getOrDefault("DATABASE_URL", "jdbc:postgresql://localhost:5432/lockstep");
        String user = env.get("DB_USER");
        String password = env.get("DB_PASSWORD");
        // Accept the common postgres://user:pass@host:port/db form as well as a JDBC URL.
        if (url.startsWith("postgres://") || url.startsWith("postgresql://")) {
            URI u = URI.create(url);
            String info = u.getRawUserInfo();
            if (info != null) {
                String[] parts = info.split(":", 2);
                if (user == null) user = decode(parts[0]);
                if (password == null && parts.length > 1) password = decode(parts[1]);
            }
            url = "jdbc:postgresql://" + u.getHost() + (u.getPort() > 0 ? ":" + u.getPort() : "") + u.getRawPath()
                    + (u.getRawQuery() != null ? "?" + u.getRawQuery() : "");
        }
        if (user == null) user = "lockstep";
        if (password == null) password = "lockstep";
        int threads = intEnv(env, "WORKER_THREADS", 4);
        return new Config(
                url,
                user,
                password,
                intEnv(env, "PORT", 8080),
                threads,
                Arrays.stream(env.getOrDefault("QUEUES", "default").split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList(),
                Duration.ofSeconds(intEnv(env, "LEASE_SECONDS", 30)),
                intEnv(env, "DB_POOL_SIZE", threads + 4));
    }

    private static int intEnv(Map<String, String> env, String key, int dflt) {
        String v = env.get(key);
        if (v == null || v.isBlank()) return dflt;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be an integer, got " + v);
        }
    }

    private static String decode(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }
}
