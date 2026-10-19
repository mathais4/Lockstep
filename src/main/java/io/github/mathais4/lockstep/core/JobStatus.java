package io.github.mathais4.lockstep.core;

import java.util.Locale;

public enum JobStatus {
    QUEUED,
    RUNNING,
    SUCCEEDED,
    /** Out of attempts or failed permanently: the dead-letter queue. */
    DEAD;

    public String db() {
        return name().toLowerCase(Locale.ROOT);
    }

    public boolean terminal() {
        return this == SUCCEEDED || this == DEAD;
    }

    public static JobStatus fromDb(String s) {
        return valueOf(s.toUpperCase(Locale.ROOT));
    }
}
