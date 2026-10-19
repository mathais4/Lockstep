package io.github.mathais4.lockstep.api;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.mathais4.lockstep.TestDb;
import io.github.mathais4.lockstep.core.JobStore;
import io.github.mathais4.lockstep.core.NewJob;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiServerTest {

    private static ApiServer server;
    private static JobStore store;
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @BeforeAll
    static void start() throws Exception {
        store = TestDb.store();
        server = new ApiServer(store, 0).start();
    }

    @AfterAll
    static void stop() {
        server.close();
    }

    @BeforeEach
    void reset() throws Exception {
        TestDb.reset();
    }

    private record Res(int status, JsonNode body, String raw) {}

    private Res call(String method, String path, String body, String... headers) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + path))
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        for (int i = 0; i + 1 < headers.length; i += 2) b.header(headers[i], headers[i + 1]);
        HttpResponse<String> r = HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
        JsonNode json = r.headers().firstValue("Content-Type").orElse("").startsWith("application/json") ? ApiServer.JSON.readTree(r.body()) : null;
        return new Res(r.statusCode(), json, r.body());
    }

    @Test
    void enqueueAndFetchAJob() throws Exception {
        Res created = call("POST", "/jobs", "{\"type\":\"sleep\",\"payload\":{\"ms\":5},\"priority\":3,\"maxAttempts\":2}");
        assertEquals(201, created.status());
        long id = created.body().get("id").asLong();
        assertEquals("queued", created.body().get("status").asText());
        assertEquals(5, created.body().path("payload").path("ms").asInt());

        Res got = call("GET", "/jobs/" + id, null);
        assertEquals(200, got.status());
        assertEquals(3, got.body().get("priority").asInt());
        assertEquals(2, got.body().get("maxAttempts").asInt());
    }

    @Test
    void repeatedIdempotencyKeyReturnsTheSameJobWith200() throws Exception {
        Res a = call("POST", "/jobs", "{\"type\":\"sleep\"}", "Idempotency-Key", "abc-1");
        Res b = call("POST", "/jobs", "{\"type\":\"sleep\"}", "Idempotency-Key", "abc-1");
        assertEquals(201, a.status());
        assertEquals(200, b.status());
        assertEquals(a.body().get("id").asLong(), b.body().get("id").asLong());
        Res c = call("POST", "/jobs", "{\"type\":\"sleep\",\"idempotencyKey\":\"abc-1\"}");
        assertEquals(200, c.status(), "key in the body works too");
    }

    @Test
    void validationErrorsAre400s() throws Exception {
        for (String body : List.of("{}", "not json", "[]", "{\"type\":\"bad type\"}", "{\"type\":\"x\",\"priority\":\"high\"}",
                "{\"type\":\"x\",\"maxAttempts\":0}", "{\"type\":\"x\",\"payload\":[1,2]}", "{\"type\":\"x\",\"delaySeconds\":-5}")) {
            Res r = call("POST", "/jobs", body);
            assertEquals(400, r.status(), body);
            assertTrue(r.body().has("error"), body);
        }
        assertEquals(400, call("GET", "/jobs?status=nope", null).status());
        assertEquals(400, call("GET", "/jobs?limit=0", null).status());
    }

    @Test
    void notFoundAndWrongMethod() throws Exception {
        assertEquals(404, call("GET", "/jobs/999999", null).status());
        assertEquals(404, call("GET", "/nope", null).status());
        assertEquals(405, call("DELETE", "/jobs", null).status());
        assertEquals(405, call("PUT", "/jobs/1", "{}").status());
    }

    @Test
    void listFiltersByStatusAndQueue() throws Exception {
        store.enqueue(NewJob.of("sleep"));
        store.enqueue(NewJob.of("sleep").withQueue("emails"));
        store.enqueue(NewJob.of("sleep").withQueue("emails"));
        assertEquals(3, call("GET", "/jobs", null).body().get("jobs").size());
        assertEquals(2, call("GET", "/jobs?queue=emails", null).body().get("jobs").size());
        assertEquals(0, call("GET", "/jobs?status=dead", null).body().get("jobs").size());
        assertEquals(1, call("GET", "/jobs?queue=emails&limit=1", null).body().get("jobs").size());
    }

    @Test
    void retryMovesADeadJobBack() throws Exception {
        long id = store.enqueue(NewJob.of("fail").withMaxAttempts(1)).job().id();
        assertEquals(409, call("POST", "/jobs/" + id + "/retry", null).status(), "not dead yet");
        store.fail(store.claim(List.of("default"), "w", 1, Duration.ofSeconds(30)).get(0), new RuntimeException("x"), true);
        Res r = call("POST", "/jobs/" + id + "/retry", null);
        assertEquals(200, r.status());
        assertEquals("queued", r.body().get("status").asText());
        assertEquals(404, call("POST", "/jobs/424242/retry", null).status());
    }

    @Test
    void statsMetricsAndHealth() throws Exception {
        store.enqueue(NewJob.of("sleep"));
        Res stats = call("GET", "/stats", null);
        assertEquals(200, stats.status());
        assertEquals(1, stats.body().path("countsByQueue").path("default").path("queued").asInt());
        Res metrics = call("GET", "/metrics", null);
        assertEquals(200, metrics.status());
        assertTrue(metrics.raw().contains("lockstep_jobs{queue=\"default\",status=\"queued\"} 1"), metrics.raw());
        assertEquals("ok", call("GET", "/health", null).body().get("status").asText());
    }
}
