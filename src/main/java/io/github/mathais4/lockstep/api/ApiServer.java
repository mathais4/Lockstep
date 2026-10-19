package io.github.mathais4.lockstep.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.mathais4.lockstep.core.Job;
import io.github.mathais4.lockstep.core.JobStatus;
import io.github.mathais4.lockstep.core.JobStore;
import io.github.mathais4.lockstep.core.NewJob;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * REST API for producers and operators, on the JDK's built-in HTTP server with one virtual
 * thread per request (no framework needed for seven routes).
 *
 * <pre>
 * POST /jobs               enqueue (201 created, or 200 with the existing job for a repeated idempotency key)
 * GET  /jobs/{id}          one job
 * GET  /jobs?status=&amp;queue=&amp;limit=
 * POST /jobs/{id}/retry    move a dead-lettered job back to the queue
 * GET  /stats              counts, throughput, latency percentiles, queue lag
 * GET  /metrics            the same in Prometheus text format
 * GET  /health             database reachable?
 * </pre>
 */
public final class ApiServer implements AutoCloseable {

    private static final Logger LOG = System.getLogger(ApiServer.class.getName());
    private static final int MAX_BODY = 1 << 20;
    private static final Pattern JOB = Pattern.compile("^/jobs/(\\d{1,18})$");
    private static final Pattern RETRY = Pattern.compile("^/jobs/(\\d{1,18})/retry$");

    static final ObjectMapper JSON = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final JobStore store;
    private final HttpServer server;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public ApiServer(JobStore store, int port) throws IOException {
        this.store = store;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(executor);
        server.createContext("/", this::handle);
    }

    public ApiServer start() {
        server.start();
        LOG.log(Level.INFO, "API listening on port {0}", port());
        return this;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(1);
        executor.shutdown();
    }

    // ------------------------------------------------------------------ routing

    private record Response(int status, Object body, String contentType) {
        static Response json(int status, Object body) {
            return new Response(status, body, "application/json");
        }
    }

    static final class HttpError extends RuntimeException {
        final int status;

        HttpError(int status, String message) {
            super(message);
            this.status = status;
        }
    }

    private void handle(HttpExchange ex) throws IOException {
        Response r;
        try {
            r = route(ex);
        } catch (HttpError e) {
            r = Response.json(e.status, Map.of("error", e.getMessage()));
        } catch (IllegalArgumentException e) {
            r = Response.json(400, Map.of("error", e.getMessage()));
        } catch (SQLException e) {
            LOG.log(Level.ERROR, "database error", e);
            r = Response.json(503, Map.of("error", "database unavailable"));
        } catch (Exception e) {
            LOG.log(Level.ERROR, "unhandled error", e);
            r = Response.json(500, Map.of("error", "internal error"));
        }
        byte[] bytes = r.body instanceof String s ? s.getBytes(StandardCharsets.UTF_8) : JSON.writeValueAsBytes(r.body);
        ex.getResponseHeaders().set("Content-Type", r.contentType + "; charset=utf-8");
        ex.sendResponseHeaders(r.status, bytes.length);
        try (OutputStream out = ex.getResponseBody()) {
            out.write(bytes);
        }
    }

    private Response route(HttpExchange ex) throws Exception {
        String method = ex.getRequestMethod();
        String path = ex.getRequestURI().getPath();
        Matcher m;

        if (path.equals("/jobs")) {
            if (method.equals("POST")) return enqueue(ex);
            if (method.equals("GET")) return list(query(ex));
            throw new HttpError(405, "method not allowed");
        }
        if ((m = JOB.matcher(path)).matches()) {
            if (!method.equals("GET")) throw new HttpError(405, "method not allowed");
            Job job = store.get(Long.parseLong(m.group(1))).orElseThrow(() -> new HttpError(404, "job not found"));
            return Response.json(200, view(job));
        }
        if ((m = RETRY.matcher(path)).matches()) {
            if (!method.equals("POST")) throw new HttpError(405, "method not allowed");
            long id = Long.parseLong(m.group(1));
            Optional<Job> retried = store.retryDead(id);
            if (retried.isPresent()) return Response.json(200, view(retried.get()));
            if (store.get(id).isEmpty()) throw new HttpError(404, "job not found");
            throw new HttpError(409, "only dead-lettered jobs can be retried");
        }
        if (path.equals("/stats") && method.equals("GET")) return Response.json(200, store.stats());
        if (path.equals("/metrics") && method.equals("GET")) return new Response(200, metrics(store.stats()), "text/plain; version=0.0.4");
        if (path.equals("/health") && method.equals("GET")) {
            store.pool().withConnection(c -> c.isValid(2));
            return Response.json(200, Map.of("status", "ok"));
        }
        throw new HttpError(404, "not found");
    }

    // ------------------------------------------------------------------ handlers

    private Response enqueue(HttpExchange ex) throws Exception {
        JsonNode body = readBody(ex);
        String type = text(body, "type", null);
        if (type == null) throw new HttpError(400, "type is required");
        JsonNode payload = body.path("payload");
        if (!payload.isMissingNode() && !payload.isObject()) throw new HttpError(400, "payload must be a JSON object");
        String key = Optional.ofNullable(ex.getRequestHeaders().getFirst("Idempotency-Key")).orElse(text(body, "idempotencyKey", null));

        NewJob job = NewJob.of(type)
                .withQueue(text(body, "queue", "default"))
                .withPayload(payload.isMissingNode() ? "{}" : JSON.writeValueAsString(payload))
                .withPriority(integer(body, "priority", 0))
                .withMaxAttempts(integer(body, "maxAttempts", 5))
                .withDelaySeconds(number(body, "delaySeconds", 0))
                .withIdempotencyKey(key);
        JobStore.EnqueueResult r = store.enqueue(job);
        return Response.json(r.created() ? 201 : 200, view(r.job()));
    }

    private Response list(Map<String, String> q) throws SQLException {
        JobStatus status = null;
        if (q.containsKey("status")) {
            try {
                status = JobStatus.fromDb(q.get("status"));
            } catch (IllegalArgumentException e) {
                throw new HttpError(400, "status must be one of queued, running, succeeded, dead");
            }
        }
        int limit;
        try {
            limit = Integer.parseInt(q.getOrDefault("limit", "50"));
        } catch (NumberFormatException e) {
            throw new HttpError(400, "limit must be an integer");
        }
        if (limit < 1 || limit > 500) throw new HttpError(400, "limit must be 1–500");
        List<Job> jobs = store.list(status, q.get("queue"), limit);
        return Response.json(200, Map.of("jobs", jobs.stream().map(ApiServer::view).toList()));
    }

    static Map<String, Object> view(Job j) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", j.id());
        m.put("queue", j.queue());
        m.put("type", j.type());
        m.put("status", j.status().db());
        m.put("priority", j.priority());
        m.put("attempts", j.attempts());
        m.put("maxAttempts", j.maxAttempts());
        m.put("payload", parse(j.payload()));
        m.put("result", parse(j.result()));
        m.put("lastError", j.lastError());
        m.put("idempotencyKey", j.idempotencyKey());
        m.put("workerId", j.workerId());
        m.put("runAt", j.runAt());
        m.put("leaseExpiresAt", j.leaseExpiresAt());
        m.put("createdAt", j.createdAt());
        m.put("firstStartedAt", j.firstStartedAt());
        m.put("completedAt", j.completedAt());
        return m;
    }

    static String metrics(JobStore.QueueStats s) {
        StringBuilder sb = new StringBuilder();
        sb.append("# HELP lockstep_jobs Jobs by queue and status.\n# TYPE lockstep_jobs gauge\n");
        s.countsByQueue().forEach((queue, byStatus) -> byStatus.forEach((status, n) ->
                sb.append("lockstep_jobs{queue=\"").append(queue).append("\",status=\"").append(status).append("\"} ").append(n).append('\n')));
        sb.append("# HELP lockstep_succeeded_last_minute Jobs completed successfully in the last 60 seconds.\n# TYPE lockstep_succeeded_last_minute gauge\n");
        sb.append("lockstep_succeeded_last_minute ").append(s.succeededLastMinute()).append('\n');
        sb.append("# HELP lockstep_latency_seconds Enqueue-to-completion time over the last 5 minutes.\n# TYPE lockstep_latency_seconds gauge\n");
        appendIfPresent(sb, "lockstep_latency_seconds{quantile=\"0.5\"}", s.latencyP50Seconds());
        appendIfPresent(sb, "lockstep_latency_seconds{quantile=\"0.95\"}", s.latencyP95Seconds());
        appendIfPresent(sb, "lockstep_latency_seconds{quantile=\"0.99\"}", s.latencyP99Seconds());
        sb.append("# HELP lockstep_oldest_ready_seconds Age of the oldest job waiting to run.\n# TYPE lockstep_oldest_ready_seconds gauge\n");
        sb.append("lockstep_oldest_ready_seconds ").append(s.oldestReadySeconds() == null ? 0 : s.oldestReadySeconds()).append('\n');
        return sb.toString();
    }

    private static void appendIfPresent(StringBuilder sb, String name, Double v) {
        if (v != null) sb.append(name).append(' ').append(v).append('\n');
    }

    // ------------------------------------------------------------------ parsing helpers

    private static JsonNode readBody(HttpExchange ex) throws IOException {
        byte[] bytes;
        try (InputStream in = ex.getRequestBody()) {
            bytes = in.readNBytes(MAX_BODY + 1);
        }
        if (bytes.length > MAX_BODY) throw new HttpError(413, "request body too large");
        if (bytes.length == 0) throw new HttpError(400, "body must be a JSON object");
        try {
            JsonNode n = JSON.readTree(bytes);
            if (n == null || !n.isObject()) throw new HttpError(400, "body must be a JSON object");
            return n;
        } catch (JsonProcessingException e) {
            throw new HttpError(400, "body is not valid JSON");
        }
    }

    private static Map<String, String> query(HttpExchange ex) {
        Map<String, String> out = new HashMap<>();
        String raw = ex.getRequestURI().getRawQuery();
        if (raw == null) return out;
        for (String kv : raw.split("&")) {
            int i = kv.indexOf('=');
            if (i > 0) out.put(URLDecoder.decode(kv.substring(0, i), StandardCharsets.UTF_8), URLDecoder.decode(kv.substring(i + 1), StandardCharsets.UTF_8));
        }
        return out;
    }

    private static String text(JsonNode n, String field, String dflt) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull()) return dflt;
        if (!v.isTextual()) throw new HttpError(400, field + " must be a string");
        return v.asText();
    }

    private static int integer(JsonNode n, String field, int dflt) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull()) return dflt;
        if (!v.isIntegralNumber() || !v.canConvertToInt()) throw new HttpError(400, field + " must be an integer");
        return v.asInt();
    }

    private static double number(JsonNode n, String field, double dflt) {
        JsonNode v = n.get(field);
        if (v == null || v.isNull()) return dflt;
        if (!v.isNumber()) throw new HttpError(400, field + " must be a number");
        return v.asDouble();
    }

    private static JsonNode parse(String json) {
        if (json == null) return null;
        try {
            return JSON.readTree(json);
        } catch (JsonProcessingException e) {
            ObjectNode o = JSON.createObjectNode();
            o.put("raw", json);
            return o;
        }
    }
}
