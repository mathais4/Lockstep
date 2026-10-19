package io.github.mathais4.lockstep.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryPolicyTest {

    private final RetryPolicy policy = new RetryPolicy(Duration.ofSeconds(1), Duration.ofSeconds(60), true);

    @Test
    void ceilingDoublesPerAttemptUpToTheCap() {
        assertEquals(1.0, policy.ceilingSeconds(1), 1e-9);
        assertEquals(2.0, policy.ceilingSeconds(2), 1e-9);
        assertEquals(32.0, policy.ceilingSeconds(6), 1e-9);
        assertEquals(60.0, policy.ceilingSeconds(7), 1e-9);
        assertEquals(60.0, policy.ceilingSeconds(1000), 1e-9);
    }

    @Test
    void fullJitterStaysWithinTheCeilingAndSpreadsOut() {
        Random rng = new Random(7);
        double min = Double.MAX_VALUE;
        double max = 0;
        for (int i = 0; i < 10_000; i++) {
            double d = policy.delaySeconds(4, rng);
            assertTrue(d >= 0 && d <= 8.0, "delay " + d + " outside [0, 8]");
            min = Math.min(min, d);
            max = Math.max(max, d);
        }
        assertTrue(min < 0.1 && max > 7.9, "jitter should cover the whole range");
    }

    @Test
    void withoutJitterTheDelayIsTheCeiling() {
        RetryPolicy fixed = new RetryPolicy(Duration.ofMillis(500), Duration.ofSeconds(10), false);
        assertEquals(4.0, fixed.delaySeconds(4, new Random()), 1e-9);
    }

    @Test
    void rejectsCapBelowBase() {
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(Duration.ofSeconds(10), Duration.ofSeconds(1), true));
    }
}
