package io.github.mathais4.lockstep.worker;

import io.github.mathais4.lockstep.core.JobHandler;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Maps job types to handlers. */
public final class HandlerRegistry {
    private final Map<String, JobHandler> handlers = new ConcurrentHashMap<>();

    public HandlerRegistry register(String type, JobHandler handler) {
        if (handlers.putIfAbsent(type, handler) != null) throw new IllegalArgumentException("handler already registered for " + type);
        return this;
    }

    public Optional<JobHandler> find(String type) {
        return Optional.ofNullable(handlers.get(type));
    }

    public Set<String> types() {
        return Set.copyOf(handlers.keySet());
    }
}
