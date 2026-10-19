package io.github.mathais4.lockstep.core;

import java.util.regex.Pattern;

/** A job to enqueue. Build with {@link #of(String)} and the {@code with…} methods. */
public record NewJob(
        String type,
        String queue,
        String payload,
        int priority,
        int maxAttempts,
        double delaySeconds,
        String idempotencyKey) {

    static final Pattern NAME = Pattern.compile("[A-Za-z0-9_.:-]{1,100}");

    public NewJob {
        if (type == null || !NAME.matcher(type).matches()) throw new IllegalArgumentException("type must match " + NAME.pattern());
        if (queue == null || !NAME.matcher(queue).matches()) throw new IllegalArgumentException("queue must match " + NAME.pattern());
        if (payload == null) payload = "{}";
        if (maxAttempts < 1 || maxAttempts > 100) throw new IllegalArgumentException("maxAttempts must be 1–100");
        if (delaySeconds < 0 || delaySeconds > 365 * 86_400) throw new IllegalArgumentException("delaySeconds must be 0–1 year");
        if (idempotencyKey != null && (idempotencyKey.isEmpty() || idempotencyKey.length() > 200)) {
            throw new IllegalArgumentException("idempotencyKey must be 1–200 characters");
        }
    }

    public static NewJob of(String type) {
        return new NewJob(type, "default", "{}", 0, 5, 0, null);
    }

    public NewJob withQueue(String q) { return new NewJob(type, q, payload, priority, maxAttempts, delaySeconds, idempotencyKey); }
    public NewJob withPayload(String json) { return new NewJob(type, queue, json, priority, maxAttempts, delaySeconds, idempotencyKey); }
    public NewJob withPriority(int p) { return new NewJob(type, queue, payload, p, maxAttempts, delaySeconds, idempotencyKey); }
    public NewJob withMaxAttempts(int n) { return new NewJob(type, queue, payload, priority, n, delaySeconds, idempotencyKey); }
    public NewJob withDelaySeconds(double s) { return new NewJob(type, queue, payload, priority, maxAttempts, s, idempotencyKey); }
    public NewJob withIdempotencyKey(String k) { return new NewJob(type, queue, payload, priority, maxAttempts, delaySeconds, k); }
}
