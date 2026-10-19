package io.github.mathais4.lockstep.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.UUID;

/** A row of the {@code jobs} table. {@code payload} and {@code result} are raw JSON text. */
public record Job(
        long id,
        String queue,
        String type,
        String payload,
        JobStatus status,
        int priority,
        int attempts,
        int maxAttempts,
        OffsetDateTime runAt,
        UUID leaseToken,
        OffsetDateTime leaseExpiresAt,
        String workerId,
        String idempotencyKey,
        String lastError,
        String result,
        OffsetDateTime createdAt,
        OffsetDateTime firstStartedAt,
        OffsetDateTime completedAt) {

    static final String COLUMNS = "id, queue, type, payload, status, priority, attempts, max_attempts, run_at, "
            + "lease_token, lease_expires_at, worker_id, idempotency_key, last_error, result, "
            + "created_at, first_started_at, completed_at";

    static Job from(ResultSet rs) throws SQLException {
        String token = rs.getString("lease_token");
        return new Job(
                rs.getLong("id"),
                rs.getString("queue"),
                rs.getString("type"),
                rs.getString("payload"),
                JobStatus.fromDb(rs.getString("status")),
                rs.getInt("priority"),
                rs.getInt("attempts"),
                rs.getInt("max_attempts"),
                rs.getObject("run_at", OffsetDateTime.class),
                token == null ? null : UUID.fromString(token),
                rs.getObject("lease_expires_at", OffsetDateTime.class),
                rs.getString("worker_id"),
                rs.getString("idempotency_key"),
                rs.getString("last_error"),
                rs.getString("result"),
                rs.getObject("created_at", OffsetDateTime.class),
                rs.getObject("first_started_at", OffsetDateTime.class),
                rs.getObject("completed_at", OffsetDateTime.class));
    }
}
