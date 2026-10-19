package io.github.mathais4.lockstep.core;

/** A failure that retrying cannot fix. The job goes straight to the dead-letter queue. */
public class NonRetryableException extends Exception {
    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
