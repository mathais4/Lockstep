package io.github.mathais4.lockstep.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.mathais4.lockstep.core.Job;
import io.github.mathais4.lockstep.core.NonRetryableException;
import io.github.mathais4.lockstep.core.Outcome;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.PreparedStatement;
import java.util.HexFormat;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Example handlers used by the demo, the tests and the load test.
 *
 * <ul>
 *   <li>{@code ledger.credit} — credits an account. The write happens in the completion
 *   transaction, so it lands exactly once. Optional {@code workMs} and {@code failRate} simulate
 *   slow and flaky work.</li>
 *   <li>{@code sleep} — waits {@code ms} milliseconds.</li>
 *   <li>{@code hash} — CPU work: {@code rounds} of SHA-256.</li>
 *   <li>{@code fail} — always fails; {@code permanent: true} makes it non-retryable.</li>
 * </ul>
 */
public final class DemoHandlers {
    private static final ObjectMapper JSON = new ObjectMapper();

    private DemoHandlers() {}

    public static HandlerRegistry registry() {
        return new HandlerRegistry()
                .register("ledger.credit", DemoHandlers::ledgerCredit)
                .register("sleep", DemoHandlers::sleep)
                .register("hash", DemoHandlers::hash)
                .register("fail", DemoHandlers::fail);
    }

    static Outcome ledgerCredit(Job job) throws Exception {
        JsonNode p = payload(job);
        String account = p.path("account").asText("");
        if (account.isEmpty() || !p.path("amount").canConvertToLong()) {
            throw new NonRetryableException("ledger.credit needs an account and an integer amount");
        }
        long amount = p.path("amount").asLong();
        simulateWork(p);
        return Outcome.withEffect("{\"credited\":" + amount + "}", tx -> {
            try (PreparedStatement ps = tx.prepareStatement("insert into ledger (job_id, account, amount) values (?, ?, ?)")) {
                ps.setLong(1, job.id());
                ps.setString(2, account);
                ps.setLong(3, amount);
                ps.executeUpdate();
            }
        });
    }

    static Outcome sleep(Job job) throws Exception {
        long ms = payload(job).path("ms").asLong(100);
        Thread.sleep(Math.max(0, Math.min(ms, 600_000)));
        return Outcome.result("{\"sleptMs\":" + ms + "}");
    }

    static Outcome hash(Job job) throws Exception {
        int rounds = (int) Math.min(payload(job).path("rounds").asLong(10_000), 10_000_000);
        MessageDigest sha = MessageDigest.getInstance("SHA-256");
        byte[] h = String.valueOf(job.id()).getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < rounds; i++) h = sha.digest(h);
        return Outcome.result("{\"digest\":\"" + HexFormat.of().formatHex(h) + "\"}");
    }

    static Outcome fail(Job job) throws Exception {
        JsonNode p = payload(job);
        if (p.path("permanent").asBoolean(false)) throw new NonRetryableException(p.path("message").asText("permanent failure"));
        throw new IllegalStateException(p.path("message").asText("transient failure"));
    }

    private static void simulateWork(JsonNode p) throws Exception {
        long workMs = p.path("workMs").asLong(0);
        if (workMs > 0) Thread.sleep(workMs);
        double failRate = p.path("failRate").asDouble(0);
        if (failRate > 0 && ThreadLocalRandom.current().nextDouble() < failRate) {
            throw new IllegalStateException("simulated transient failure");
        }
    }

    private static JsonNode payload(Job job) throws NonRetryableException {
        try {
            return JSON.readTree(job.payload());
        } catch (Exception e) {
            throw new NonRetryableException("payload is not valid JSON", e);
        }
    }
}
