package com.hsm.core.loadtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.http.HttpDsl.*;

/**
 * Standalone Gatling load test against a running hsm-core-service, answering
 * java/docs/PERFORMANCE_TESTING.md's open question: real request-rate/latency
 * numbers for /encrypt, /decrypt, /encrypt/batch, and /decrypt/batch. NOT a JUnit
 * test -- like BulkVsBatchBenchmark (hsm-bulk-service), this drives real HTTP
 * traffic against a service that must already be running and is never invoked as
 * part of the normal `mvn test`/`mvn package` lifecycle (see the gatling-maven-plugin
 * entry in pom.xml, deliberately bound to no execution phase).
 *
 * <h2>Prerequisites</h2>
 * <pre>
 *   # terminal 1 -- hsm-core-service, demo mode (MockKekClient -- see the
 *   # "what this measures" note below for why that matters)
 *   DEMO_MODE=true java -jar hsm-core-service/target/hsm-core-service.jar
 *
 *   # terminal 2
 *   mvn -pl hsm-core-service gatling:test \
 *     -Dgatling.simulationClass=com.hsm.core.loadtest.EncryptDecryptLoadSimulation
 * </pre>
 *
 * <p>Report lands at {@code hsm-core-service/target/gatling/<run-id>/index.html}.
 *
 * <h2>Tuning (system properties, all optional)</h2>
 * <ul>
 *   <li>{@code hsm.baseUrl} (default {@code http://localhost:3005})
 *   <li>{@code hsm.appId} / {@code hsm.token} (default {@code payments-svc} /
 *       {@code demo-token-payments-svc} -- {@code MockJwtValidator}'s seeded demo
 *       app with both encrypt and decrypt scopes)
 *   <li>{@code hsm.singleUsers} / {@code hsm.batchUsers} -- concurrent virtual users
 *       for each scenario (default 20 / 5)
 *   <li>{@code hsm.batchSize} -- items per batch call (default 20, capped by the
 *       server's own {@code hsm.service.batch-max-items}, default 100)
 *   <li>{@code hsm.rampSeconds} / {@code hsm.holdSeconds} -- ramp-up then
 *       constant-rate hold duration (default 10 / 30)
 * </ul>
 *
 * <p><b>What this measures, and what it doesn't:</b> demo mode runs against
 * {@code MockKekClient} (no real Azure Managed HSM call) and H2, not Postgres --
 * so this isolates hsm-core-service's own overhead (JSON, Bean Validation, PBAC
 * check, DB write/read, AOP timing) under concurrency, not real-infra absolute
 * throughput. Batch scenario numbers are also capped by
 * {@code hsm.service.batch-executor-pool-size} (default 1, meaning fully
 * sequential item processing inside one batch call) -- see
 * {@code java/docs/BULK_OPERATIONS.md}'s bounded-concurrency section before
 * reading a low batch-items/sec number as a Gatling-side bottleneck rather than
 * a deliberate server-side throttle.
 */
public class EncryptDecryptLoadSimulation extends Simulation {

    private static final String BASE_URL = System.getProperty("hsm.baseUrl", "http://localhost:3005");
    private static final String API_PREFIX = "/api/sensec/hsm/v1";
    private static final String APP_ID = System.getProperty("hsm.appId", "payments-svc");
    private static final String TOKEN = System.getProperty("hsm.token", "demo-token-" + APP_ID);

    private static final int SINGLE_USERS = Integer.getInteger("hsm.singleUsers", 20);
    private static final int BATCH_USERS = Integer.getInteger("hsm.batchUsers", 5);
    private static final int BATCH_SIZE = Integer.getInteger("hsm.batchSize", 20);
    private static final int RAMP_SECONDS = Integer.getInteger("hsm.rampSeconds", 10);
    private static final int HOLD_SECONDS = Integer.getInteger("hsm.holdSeconds", 30);

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicInteger SEQ = new AtomicInteger();

    private final HttpProtocolBuilder httpProtocol = http
            .baseUrl(BASE_URL)
            .acceptHeader("application/json")
            .contentTypeHeader("application/json")
            .header("Authorization", "Bearer " + TOKEN)
            .header("X-App-ID", APP_ID);

    private final ScenarioBuilder singleRoundTrip = scenario("Single encrypt + decrypt")
            .exec(
                    http("POST /encrypt")
                            .post(API_PREFIX + "/encrypt")
                            .body(StringBody(session ->
                                    "{\"plaintext\": \"load-test-value-" + SEQ.incrementAndGet() + "\"}"))
                            .check(status().is(201)) // EncryptController returns 201 Created, not 200
                            .check(jsonPath("$.ciphertext").saveAs("ciphertext"))
            )
            .exec(
                    http("POST /decrypt")
                            .post(API_PREFIX + "/decrypt")
                            .body(StringBody(session ->
                                    "{\"ciphertext\": \"" + session.getString("ciphertext") + "\"}"))
                            .check(status().is(200))
            );

    private final ScenarioBuilder batchRoundTrip = scenario("Batch encrypt + decrypt")
            .exec(
                    http("POST /encrypt/batch")
                            .post(API_PREFIX + "/encrypt/batch")
                            .body(StringBody(session -> batchEncryptRequestBody()))
                            .check(status().is(200))
                            .check(bodyString().saveAs("encryptBatchResponseBody"))
            )
            .exec(session -> session.set(
                    "decryptBatchRequestBody",
                    batchDecryptRequestBody(session.getString("encryptBatchResponseBody"))))
            .exec(
                    http("POST /decrypt/batch")
                            .post(API_PREFIX + "/decrypt/batch")
                            .body(StringBody(session -> session.getString("decryptBatchRequestBody")))
                            .check(status().is(200))
            );

    private static String batchEncryptRequestBody() {
        StringBuilder items = new StringBuilder();
        for (int i = 0; i < BATCH_SIZE; i++) {
            if (i > 0) {
                items.append(',');
            }
            items.append("{\"key\": \"row-").append(i).append("\", \"plaintext\": \"batch-load-value-")
                    .append(SEQ.incrementAndGet()).append('-').append(i).append("\"}");
        }
        return "{\"items\": [" + items + "]}";
    }

    /** Chains batch decrypt off the just-completed batch encrypt's own ciphertexts, same round-trip shape as the single scenario above. */
    private static String batchDecryptRequestBody(String encryptBatchResponseJson) {
        try {
            JsonNode root = MAPPER.readTree(encryptBatchResponseJson);
            ArrayNode items = MAPPER.createArrayNode();
            int i = 0;
            for (JsonNode resultItem : root.get("items")) {
                ObjectNode decryptItem = items.addObject();
                decryptItem.put("key", "row-" + i);
                decryptItem.put("ciphertext", resultItem.get("result").get("ciphertext").asText());
                i++;
            }
            ObjectNode body = MAPPER.createObjectNode();
            body.set("items", items);
            return MAPPER.writeValueAsString(body);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to build /decrypt/batch request from /encrypt/batch response: " + encryptBatchResponseJson, e);
        }
    }

    {
        setUp(
                singleRoundTrip.injectOpen(
                        rampUsers(SINGLE_USERS).during(Duration.ofSeconds(RAMP_SECONDS)),
                        constantUsersPerSec(SINGLE_USERS).during(Duration.ofSeconds(HOLD_SECONDS))
                ),
                batchRoundTrip.injectOpen(
                        rampUsers(BATCH_USERS).during(Duration.ofSeconds(RAMP_SECONDS)),
                        constantUsersPerSec(BATCH_USERS).during(Duration.ofSeconds(HOLD_SECONDS))
                )
        ).protocols(httpProtocol);
    }
}
