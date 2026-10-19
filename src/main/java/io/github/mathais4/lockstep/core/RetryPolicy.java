package io.github.mathais4.lockstep.core;

import java.time.Duration;
import java.util.random.RandomGenerator;

/**
 * Exponential backoff with "full jitter": the delay before retry n is drawn uniformly from
 * [0, min(cap, base · 2^(n−1))]. Jitter spreads retries out so a burst of failures (say, a
 * downstream outage) doesn't come back as a synchronised burst of retries.
 */
public record RetryPolicy(Duration base, Duration cap, boolean jitter) {

    public static final RetryPolicy DEFAULT = new RetryPolicy(Duration.ofSeconds(1), Duration.ofMinutes(10), true);

    public RetryPolicy {
        if (base.isNegative() || cap.isNegative() || cap.compareTo(base) < 0) throw new IllegalArgumentException("need 0 <= base <= cap");
    }

    /** Upper bound of the delay before the retry that follows failed attempt {@code attempt} (1-based). */
    public double ceilingSeconds(int attempt) {
        int exp = Math.min(Math.max(attempt, 1) - 1, 40);
        double raw = base.toMillis() / 1000.0 * Math.pow(2, exp);
        return Math.min(raw, cap.toMillis() / 1000.0);
    }

    public double delaySeconds(int attempt, RandomGenerator rng) {
        double ceiling = ceilingSeconds(attempt);
        return jitter ? rng.nextDouble() * ceiling : ceiling;
    }
}
