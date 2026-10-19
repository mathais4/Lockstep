package io.github.mathais4.lockstep.core;

/**
 * Does the work for one job type.
 *
 * <p>Delivery is at-least-once: a job can run again if its worker crashes or stalls. Work
 * outside the database should therefore be idempotent. Database writes that must happen once
 * belong in {@link Outcome#withEffect}, which commits atomically with the job's completion.
 *
 * <p>Throw {@link NonRetryableException} for failures retrying cannot fix (bad input);
 * any other exception is retried with backoff until the job runs out of attempts.
 */
@FunctionalInterface
public interface JobHandler {
    Outcome handle(Job job) throws Exception;
}
