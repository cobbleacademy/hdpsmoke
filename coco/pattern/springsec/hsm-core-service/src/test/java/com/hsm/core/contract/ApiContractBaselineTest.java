package com.hsm.core.contract;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hsm.core.model.AppRegistration;
import com.hsm.core.repository.AppRegistrationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the externally visible shape of hsm-core-service's API -- the contract that
 * hsm-bulk-client, hsm-file-service, the Databricks UDF and the Spark adapter depend on
 * -- so a refactor (the chassis work in particular) can't change it unnoticed.
 *
 * <p>Each case records the HTTP status, whether the contract headers are present, and the
 * JSON body's <em>shape</em>: field names and JSON types, recursively, with each array
 * reduced to the shape of its first element. Values are not recorded -- ciphertexts, ids
 * and timestamps are random per run. Baselines live in
 * {@code src/test/resources/contract-baseline/}, one file per case.
 *
 * <p>A mismatch is a client-visible change. If it is intended, regenerate the baselines and
 * review the diff like any other contract change:
 * <pre>mvn -pl hsm-core-service test -Dtest=ApiContractBaselineTest -Dcontract.baseline.update=true</pre>
 * See java/docs/COMPATIBILITY_CONTRACT.md.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("demo")
class ApiContractBaselineTest {

    private static final String PREFIX = "/api/sensec/hsm/v1";
    private static final Path BASELINE_DIR = Path.of("src/test/resources/contract-baseline");
    private static final boolean UPDATE = Boolean.getBoolean("contract.baseline.update");
    /** Headers whose presence (not value) is part of the contract. */
    private static final List<String> CONTRACT_HEADERS = List.of("X-Correlation-Id", "Content-Type");
    private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:contract-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private AppRegistrationRepository appRegistrationRepository;

    /** /dek/issue wraps each DEK to the caller's registered public key, so payments-svc needs one. */
    @BeforeEach
    void registerPaymentsPublicKey() throws Exception {
        AppRegistration payments = appRegistrationRepository.findById("payments-svc").orElseThrow();
        if (payments.getPublicKeyPem() == null) {
            KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
            gen.initialize(2048);
            byte[] der = gen.generateKeyPair().getPublic().getEncoded();
            payments.setPublicKeyPem("-----BEGIN PUBLIC KEY-----\n" + Base64.getEncoder().encodeToString(der) + "\n-----END PUBLIC KEY-----\n");
            appRegistrationRepository.save(payments);
        }
    }

    private static HttpHeaders auth(String app) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("Authorization", "Bearer demo-token-" + app);
        h.set("X-App-ID", app);
        return h;
    }

    private ResponseEntity<String> post(String path, Object body, HttpHeaders headers) {
        return rest.exchange(PREFIX + path, HttpMethod.POST, new HttpEntity<>(body, headers), String.class);
    }

    private JsonNode body(ResponseEntity<String> response) throws IOException {
        return JSON.readTree(response.getBody());
    }

    private String encryptToken(String plaintext) throws IOException {
        return body(post("/encrypt", Map.of("plaintext", plaintext), auth("payments-svc"))).get("ciphertext").asText();
    }

    // ── Happy paths: the calls clients actually make ────────────────────────────

    @Test
    void encrypt() throws IOException {
        check("encrypt", post("/encrypt", Map.of("plaintext", "contract", "data_classification", "pii"), auth("payments-svc")));
    }

    @Test
    void encryptWithDekName() throws IOException {
        check("encrypt-dek-name", post("/encrypt",
                Map.of("plaintext", "contract", "dek_name", "contract.baseline-" + System.nanoTime()), auth("payments-svc")));
    }

    @Test
    void decrypt() throws IOException {
        check("decrypt", post("/decrypt", Map.of("ciphertext", encryptToken("contract")), auth("payments-svc")));
    }

    @Test
    void encryptBatch() throws IOException {
        check("encrypt-batch", post("/encrypt/batch",
                Map.of("items", List.of(Map.of("key", "row-1", "plaintext", "a"), Map.of("key", "row-2", "plaintext", "b"))),
                auth("payments-svc")));
    }

    @Test
    void decryptBatch() throws IOException {
        check("decrypt-batch", post("/decrypt/batch",
                Map.of("items", List.of(Map.of("key", "row-1", "ciphertext", encryptToken("a")))), auth("payments-svc")));
    }

    @Test
    void dekIssue() throws IOException {
        check("dek-issue", post("/dek/issue", Map.of("items", List.of(Map.of("key", "col-1"))), auth("payments-svc")));
    }

    @Test
    void dekUnwrap() throws IOException {
        JsonNode issued = body(post("/dek/issue", Map.of("items", List.of(Map.of("key", "col-1"))), auth("payments-svc")));
        String edekId = issued.get("items").get(0).get("edek_id").asText();
        check("dek-unwrap", post("/dek/unwrap",
                Map.of("items", List.of(Map.of("key", "col-1", "edek_id", edekId))), auth("payments-svc")));
    }

    @Test
    void adminHealth() throws IOException {
        check("admin-health", rest.getForEntity(PREFIX + "/admin/health", String.class));
    }

    @Test
    void adminRotateKek() throws IOException {
        encryptToken("contract"); // at least one EDEK, so "results" is non-empty regardless of test order
        check("admin-rotate-kek", post("/admin/rotate-kek", null, auth("ops-admin")));
    }

    // ── Error paths: clients parse these too ────────────────────────────────────

    @Test
    void errorMissingToken() throws IOException {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("X-App-ID", "payments-svc");
        check("error-401-missing-token", post("/encrypt", Map.of("plaintext", "x"), h));
    }

    @Test
    void errorScopeDenied() throws IOException {
        check("error-403-scope", post("/encrypt", Map.of("plaintext", "x"), auth("reporting-app")));
    }

    @Test
    void errorValidation() throws IOException {
        check("error-422-validation", post("/encrypt", Map.of("plaintext", ""), auth("payments-svc")));
    }

    @Test
    void errorTamperedCiphertext() throws IOException {
        String token = encryptToken("contract");
        String tampered = token.substring(0, token.length() - 4) + (token.endsWith("AAAA") ? "BBBB" : "AAAA");
        check("error-decrypt-tampered", post("/decrypt", Map.of("ciphertext", tampered), auth("payments-svc")));
    }

    @Test
    void errorMalformedCiphertext() throws IOException {
        check("error-decrypt-malformed", post("/decrypt", Map.of("ciphertext", "not-a-token"), auth("payments-svc")));
    }

    @Test
    void errorUnknownPath() throws IOException {
        check("error-404-path", post("/no-such-endpoint", Map.of(), auth("payments-svc")));
    }

    // ── Shape extraction and comparison ─────────────────────────────────────────

    private void check(String name, ResponseEntity<String> response) throws IOException {
        ObjectNode actual = JsonNodeFactory.instance.objectNode();
        actual.put("status", response.getStatusCode().value());
        ObjectNode headers = actual.putObject("headers_present");
        for (String header : CONTRACT_HEADERS) {
            headers.put(header, response.getHeaders().containsHeader(header));
        }
        String raw = response.getBody();
        actual.set("body", raw == null || raw.isBlank() ? JsonNodeFactory.instance.nullNode() : shapeOf(JSON.readTree(raw)));
        String rendered = JSON.writeValueAsString(actual) + "\n";

        Path file = BASELINE_DIR.resolve(name + ".json");
        if (UPDATE) {
            Files.createDirectories(BASELINE_DIR);
            Files.writeString(file, rendered);
            return;
        }
        assertTrue(Files.exists(file), "No baseline " + file + " -- run with -Dcontract.baseline.update=true and commit it");
        assertEquals(Files.readString(file), rendered,
                "Client-visible API shape changed for '" + name + "'. If intended, regenerate with "
                        + "-Dcontract.baseline.update=true and review the diff (java/docs/COMPATIBILITY_CONTRACT.md).");
    }

    /** Field names and JSON types only; arrays reduced to their first element's shape; object keys sorted. */
    static JsonNode shapeOf(JsonNode node) {
        JsonNodeFactory f = JsonNodeFactory.instance;
        if (node.isObject()) {
            Map<String, JsonNode> sorted = new TreeMap<>();
            for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> e = it.next();
                sorted.put(e.getKey(), shapeOf(e.getValue()));
            }
            ObjectNode out = f.objectNode();
            sorted.forEach(out::set);
            return out;
        }
        if (node.isArray()) {
            ArrayNode out = f.arrayNode();
            if (!node.isEmpty()) {
                out.add(shapeOf(node.get(0)));
            }
            return out;
        }
        if (node.isTextual()) {
            return f.textNode("string");
        }
        if (node.isNumber()) {
            return f.textNode("number");
        }
        if (node.isBoolean()) {
            return f.textNode("boolean");
        }
        return f.textNode("null");
    }
}
