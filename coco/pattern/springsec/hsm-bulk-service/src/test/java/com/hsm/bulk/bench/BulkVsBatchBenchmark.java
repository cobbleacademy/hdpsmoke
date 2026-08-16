package com.hsm.bulk.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hsm.bulk.config.FipsBootstrap;
import com.hsm.bulk.crypto.DekManager;
import com.hsm.bulk.crypto.TransportWrapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Standalone CLNT-side reference client + benchmark for java/docs/BULK_OPERATIONS.md's
 * Tier 3 PoC -- NOT a JUnit assertion test, a runnable measurement tool. Compares:
 *
 * <p><b>Batch (Tier 1)</b>: plaintext travels to hsm-core-service, which does the
 * AES-GCM encrypt itself, via POST /encrypt/batch.
 *
 * <p><b>Bulk (Tier 3)</b>: hsm-bulk-service only issues a transport-wrapped DEK via
 * POST /dek/issue; this class plays CLNT -- unwraps it locally (its own RSA private
 * key, never sent anywhere) and runs the AES-GCM encrypt itself, via the same
 * DekManager class hsm-core-service uses internally.
 *
 * <h2>Prerequisites (both run locally, demo-mode-equivalent, no real Azure needed)</h2>
 * <pre>
 *   # terminal 1 -- hsm-core-service, demo mode, H2 file shared via AUTO_SERVER
 *   DEMO_MODE=true DEMO_DATABASE_URL="jdbc:h2:file:./demo_hsm_h2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;AUTO_SERVER=TRUE" \
 *     java -jar hsm-core-service/target/hsm-core-service.jar
 *
 *   # terminal 2 -- hsm-bulk-service (application.yml's own default already points at
 *   # the same file + AUTO_SERVER=TRUE)
 *   java -jar hsm-bulk-service/target/hsm-bulk-service.jar
 *
 *   # terminal 3
 *   mvn -pl hsm-bulk-service test-compile exec:java -Dexec.mainClass=com.hsm.bulk.bench.BulkVsBatchBenchmark -Dexec.classpathScope=test
 * </pre>
 *
 * <p>hsm-core-service's own DemoSeedInitializer must have already run (i.e. it must
 * be up first) so the {@code payments-svc} app_registrations row this benchmark
 * updates in place already exists.
 */
public final class BulkVsBatchBenchmark {

    private static final String CORE_SERVICE_URL = System.getProperty("coreServiceUrl", "http://localhost:3005");
    private static final String BULK_SERVICE_URL = System.getProperty("bulkServiceUrl", "http://localhost:3006");
    private static final String H2_JDBC_URL = System.getProperty("h2JdbcUrl",
            "jdbc:h2:file:./demo_hsm_h2;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;AUTO_SERVER=TRUE");
    private static final String APP_ID = "payments-svc";
    private static final String TOKEN = "demo-token-payments-svc";
    private static final String API_PREFIX = "/api/sensec/hsm/v1";
    private static final int RECORD_COUNT = Integer.getInteger("recordCount", 200);
    private static final int CHUNK_SIZE = 100;

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private BulkVsBatchBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        FipsBootstrap.register();

        System.out.println("=== Tier 1 (Batch) vs Tier 3 (Bulk) PoC benchmark ===");
        System.out.println("records=" + RECORD_COUNT + " chunkSize=" + CHUNK_SIZE
                + " coreService=" + CORE_SERVICE_URL + " bulkService=" + BULK_SERVICE_URL);

        System.out.println("\n-- Provisioning: generating CLNT RSA-2048 keypair, registering it on '" + APP_ID + "' --");
        KeyPair keyPair = generateKeyPair();
        provisionAppKeyAndScopes(keyPair);

        System.out.println("\n-- Token-format compatibility check (must pass before the numbers below mean anything) --");
        verifyTokenFormatCompatibility(keyPair.getPrivate());
        System.out.println("PASS: a /dek/issue-derived DEK, used to locally encrypt via DekManager, decrypted correctly through hsm-core-service's real /decrypt.");

        List<String> plaintexts = new ArrayList<>(RECORD_COUNT);
        for (int i = 0; i < RECORD_COUNT; i++) {
            plaintexts.add("benchmark-record-" + i + "-" + "x".repeat(200));
        }

        System.out.println("\n-- Running Batch path (POST /encrypt/batch) --");
        long batchMs = runBatchPath(plaintexts);
        report("Batch", batchMs, RECORD_COUNT);

        System.out.println("\n-- Running Bulk path (POST /dek/issue + local AES-GCM) --");
        long bulkMs = runBulkPath(plaintexts, keyPair.getPrivate());
        report("Bulk", bulkMs, RECORD_COUNT);

        System.out.println("\n-- Summary --");
        System.out.printf("Batch: %6d ms total, %8.1f items/sec%n", batchMs, RECORD_COUNT * 1000.0 / batchMs);
        System.out.printf("Bulk:  %6d ms total, %8.1f items/sec%n", bulkMs, RECORD_COUNT * 1000.0 / bulkMs);
        System.out.println("\nNote: both services run against MockKekClient locally (no real Azure Managed HSM call),");
        System.out.println("so this measures relative shape (SVC-mediated AES-GCM vs. CLNT-local AES-GCM under an");
        System.out.println("otherwise-identical HTTP/auth/DB path), not real-infra absolute throughput -- see");
        System.out.println("BULK_OPERATIONS.md's Phase 6 for the real-infra pilot this would still need.");
    }

    private static KeyPair generateKeyPair() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        return gen.generateKeyPair();
    }

    private static void provisionAppKeyAndScopes(KeyPair keyPair) throws Exception {
        String pem = "-----BEGIN PUBLIC KEY-----\n"
                + Base64.getEncoder().encodeToString(keyPair.getPublic().getEncoded())
                + "\n-----END PUBLIC KEY-----\n";
        try (Connection conn = DriverManager.getConnection(H2_JDBC_URL, "sa", "")) {
            try (PreparedStatement select = conn.prepareStatement(
                    "SELECT allowed_scopes FROM app_registrations WHERE app_id = ?")) {
                select.setString(1, APP_ID);
                var rs = select.executeQuery();
                if (!rs.next()) {
                    throw new IllegalStateException(
                            "app_registrations row for '" + APP_ID + "' not found -- start hsm-core-service in "
                                    + "DEMO_MODE=true first so DemoSeedInitializer creates it, then re-run this benchmark.");
                }
                String existingScopes = rs.getString(1);
                String newScopes = existingScopes.contains("dek_issue") ? existingScopes : existingScopes + ",dek_issue,dek_unwrap";
                try (PreparedStatement update = conn.prepareStatement(
                        "UPDATE app_registrations SET allowed_scopes = ?, public_key_pem = ? WHERE app_id = ?")) {
                    update.setString(1, newScopes);
                    update.setString(2, pem);
                    update.setString(3, APP_ID);
                    update.executeUpdate();
                }
            }
        }
        System.out.println("Provisioned '" + APP_ID + "' with dek_issue/dek_unwrap scopes and a public key.");
    }

    /** BULK_OPERATIONS.md's Tier 3 hard requirement: a /dek/issue-derived ciphertext token must decrypt through hsm-core-service's real, unmodified /decrypt. */
    private static void verifyTokenFormatCompatibility(PrivateKey privateKey) throws Exception {
        ObjectNode issueBody = MAPPER.createObjectNode();
        ArrayNode items = issueBody.putArray("items");
        items.addObject().put("key", "verify-1");

        JsonNode issueResponse = postJson(BULK_SERVICE_URL + API_PREFIX + "/dek/issue", issueBody);
        JsonNode item = issueResponse.get("items").get(0);
        if (!"success".equals(item.get("status").asText())) {
            throw new IllegalStateException("dek/issue verification item failed: " + item);
        }
        String edekId = item.get("edek_id").asText();
        byte[] wrappedDek = Base64.getDecoder().decode(item.get("wrapped_dek_b64").asText());
        byte[] dek = TransportWrapper.unwrap(wrappedDek, privateKey);

        String plaintext = "token-format-compat-check";
        DekManager.EncryptResult encrypted;
        try {
            encrypted = DekManager.encrypt(plaintext.getBytes(StandardCharsets.UTF_8), dek, APP_ID);
        } finally {
            DekManager.zeroDek(dek);
        }
        String ciphertext = DekManager.packToken(
                java.util.UUID.fromString(edekId), encrypted.iv(), encrypted.tag(), encrypted.ciphertext());

        ObjectNode decryptBody = MAPPER.createObjectNode();
        decryptBody.put("ciphertext", ciphertext);
        JsonNode decryptResponse = postJson(CORE_SERVICE_URL + API_PREFIX + "/decrypt", decryptBody);
        String decrypted = decryptResponse.get("plaintext").asText();
        if (!plaintext.equals(decrypted)) {
            throw new IllegalStateException("Token-format compatibility FAILED: expected '" + plaintext + "', got '" + decrypted + "'");
        }
    }

    private static long runBatchPath(List<String> plaintexts) throws Exception {
        long start = System.currentTimeMillis();
        for (int offset = 0; offset < plaintexts.size(); offset += CHUNK_SIZE) {
            List<String> chunk = plaintexts.subList(offset, Math.min(offset + CHUNK_SIZE, plaintexts.size()));
            ObjectNode body = MAPPER.createObjectNode();
            ArrayNode items = body.putArray("items");
            for (int i = 0; i < chunk.size(); i++) {
                ObjectNode item = items.addObject();
                item.put("key", "row-" + (offset + i));
                item.put("plaintext", chunk.get(i));
            }
            postJson(CORE_SERVICE_URL + API_PREFIX + "/encrypt/batch", body);
        }
        return System.currentTimeMillis() - start;
    }

    private static long runBulkPath(List<String> plaintexts, PrivateKey privateKey) throws Exception {
        long start = System.currentTimeMillis();
        for (int offset = 0; offset < plaintexts.size(); offset += CHUNK_SIZE) {
            List<String> chunk = plaintexts.subList(offset, Math.min(offset + CHUNK_SIZE, plaintexts.size()));
            ObjectNode body = MAPPER.createObjectNode();
            ArrayNode items = body.putArray("items");
            for (int i = 0; i < chunk.size(); i++) {
                items.addObject().put("key", "row-" + (offset + i));
            }
            JsonNode response = postJson(BULK_SERVICE_URL + API_PREFIX + "/dek/issue", body);

            int i = 0;
            for (JsonNode item : response.get("items")) {
                if (!"success".equals(item.get("status").asText())) {
                    throw new IllegalStateException("dek/issue item failed: " + item);
                }
                byte[] wrappedDek = Base64.getDecoder().decode(item.get("wrapped_dek_b64").asText());
                byte[] dek = TransportWrapper.unwrap(wrappedDek, privateKey);
                try {
                    // CLNT's local compute cost -- this is the work Tier 1 does inside hsm-core-service instead.
                    DekManager.encrypt(chunk.get(i).getBytes(StandardCharsets.UTF_8), dek, APP_ID);
                } finally {
                    DekManager.zeroDek(dek);
                }
                i++;
            }
        }
        return System.currentTimeMillis() - start;
    }

    private static void report(String label, long ms, int count) {
        System.out.printf("%s: %d items in %d ms (%.1f items/sec)%n", label, count, ms, count * 1000.0 / ms);
    }

    private static JsonNode postJson(String url, ObjectNode body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + TOKEN)
                .header("X-App-ID", APP_ID)
                .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)))
                .build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 300) {
            throw new IllegalStateException("POST " + url + " -> HTTP " + response.statusCode() + ": " + response.body());
        }
        return MAPPER.readTree(response.body());
    }
}
