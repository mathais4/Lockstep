package io.github.mathais4.lockstep.core;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * What a handler returns: an optional JSON result, and an optional database effect.
 *
 * <p>The effect runs in the same transaction that marks the job succeeded, after the
 * fencing check. So it is applied exactly once: if this worker has lost the job (its lease
 * expired and another worker took over), the whole transaction rolls back.
 */
public record Outcome(String resultJson, SqlEffect effect) {

    @FunctionalInterface
    public interface SqlEffect {
        void apply(Connection tx) throws SQLException;
    }

    public static Outcome ok() {
        return new Outcome(null, null);
    }

    public static Outcome result(String json) {
        return new Outcome(json, null);
    }

    public static Outcome withEffect(String json, SqlEffect effect) {
        return new Outcome(json, effect);
    }
}
