package com.hsm.core;

import com.hsm.crypto.DekManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The v2 API (hsm.service.api-v2-prefix): the same endpoints as v1 with camelCase JSON
 * fields, unknown fields rejected, and exactly v1's authorization -- see
 * ApiVersionJsonConfig. v1's own shape is pinned separately by ApiContractBaselineTest.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("demo")
class ApiV2IntegrationTest {

    private static final String V1 = "/api/sensec/hsm/v1";
    private static final String V2 = "/api/sensec/hsm/v2";

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:hsmv2-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private TestRestTemplate rest;

    private static HttpHeaders headers(String token, String appId) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        if (token != null) {
            h.set("Authorization", "Bearer " + token);
        }
        h.set("X-App-ID", appId);
        h.set("X-Response-Detail", "full");
        return h;
    }

    private static HttpHeaders payments() {
        return headers("demo-token-payments-svc", "payments-svc");
    }

    private ResponseEntity<Map> post(String path, Object body, HttpHeaders headers) {
        return rest.postForEntity(path, new HttpEntity<>(body, headers), Map.class);
    }

    @Test
    void camelCaseRoundTrip() {
        ResponseEntity<Map> enc = post(V2 + "/encrypt",
                Map.of("plaintext", "v2 secret", "dataClassification", "pii", "dekName", "v2-roundtrip"), payments());
        assertEquals(HttpStatus.CREATED, enc.getStatusCode());
        Map encBody = enc.getBody();
        assertTrue(encBody.containsKey("edekId"), "camelCase response fields: " + encBody.keySet());
        assertFalse(encBody.keySet().stream().anyMatch(k -> ((String) k).contains("_")), "no snake_case: " + encBody.keySet());

        ResponseEntity<Map> dec = post(V2 + "/decrypt", Map.of("ciphertext", encBody.get("ciphertext")), payments());
        assertEquals(HttpStatus.OK, dec.getStatusCode());
        assertEquals("v2 secret", dec.getBody().get("plaintext"));
        assertEquals("payments-svc", dec.getBody().get("ownerAppId"));
        assertFalse(dec.getBody().containsKey("owner_app_id"));
    }

    @Test
    void v1AndV2ShareData() {
        // Same service, same records: a v1 ciphertext decrypts through v2 and vice versa.
        String fromV1 = (String) post(V1 + "/encrypt", Map.of("plaintext", "one"), payments()).getBody().get("ciphertext");
        String fromV2 = (String) post(V2 + "/encrypt", Map.of("plaintext", "two"), payments()).getBody().get("ciphertext");
        assertEquals("one", post(V2 + "/decrypt", Map.of("ciphertext", fromV1), payments()).getBody().get("plaintext"));
        assertEquals("two", post(V1 + "/decrypt", Map.of("ciphertext", fromV2), payments()).getBody().get("plaintext"));
    }

    @Test
    void camelCaseMultiWordRequestFieldsAreRead() {
        // Proves edekId/ivB64/... are bound, not silently dropped (which would fail the decrypt).
        String token = (String) post(V2 + "/encrypt", Map.of("plaintext", "legacy v2"), payments()).getBody().get("ciphertext");
        DekManager.UnpackedToken unpacked = DekManager.unpackToken(token);
        ResponseEntity<Map> dec = post(V2 + "/decrypt", Map.of(
                "edekId", unpacked.edekId().toString(),
                "ivB64", Base64.getEncoder().encodeToString(unpacked.iv()),
                "ciphertextB64", Base64.getEncoder().encodeToString(unpacked.ciphertext()),
                "tagB64", Base64.getEncoder().encodeToString(unpacked.tag())), payments());
        assertEquals(HttpStatus.OK, dec.getStatusCode());
        assertEquals("legacy v2", dec.getBody().get("plaintext"));
    }

    @Test
    void snakeCaseFieldOnV2IsRejected() {
        ResponseEntity<Map> resp = post(V2 + "/encrypt", Map.of("plaintext", "x", "dek_name", "oops"), payments());
        assertEquals(HttpStatus.BAD_REQUEST, resp.getStatusCode());
        assertEquals("Unknown field 'dek_name'", resp.getBody().get("detail"));
    }

    @Test
    void unknownFieldOnV1IsStillIgnored() {
        ResponseEntity<Map> resp = post(V1 + "/encrypt", Map.of("plaintext", "x", "dekName", "ignored-on-v1"), payments());
        assertEquals(HttpStatus.CREATED, resp.getStatusCode());
        assertTrue(resp.getBody().containsKey("edek_id"), "v1 stays snake_case: " + resp.getBody().keySet());
    }

    @Test
    void malformedJsonIs400OnBothVersions() {
        for (String prefix : List.of(V1, V2)) {
            ResponseEntity<Map> resp = rest.exchange(prefix + "/encrypt", HttpMethod.POST,
                    new HttpEntity<>("{not json", payments()), Map.class);
            assertEquals(HttpStatus.BAD_REQUEST, resp.getStatusCode(), prefix);
            assertEquals("Malformed request body", resp.getBody().get("detail"), prefix);
        }
    }

    @Test
    void v2HasV1Authorization() {
        // Without the access rules applied to v2 these would fall through to permitAll().
        assertEquals(HttpStatus.UNAUTHORIZED,
                post(V2 + "/encrypt", Map.of("plaintext", "x"), headers(null, "payments-svc")).getStatusCode());
        assertEquals(HttpStatus.FORBIDDEN,
                post(V2 + "/encrypt", Map.of("plaintext", "x"), headers("demo-token-reporting-app", "reporting-app")).getStatusCode());
        assertEquals(HttpStatus.UNAUTHORIZED,
                rest.exchange(V2 + "/admin/grants", HttpMethod.GET, new HttpEntity<>(headers(null, "ops-admin")), Map.class).getStatusCode());
        assertEquals(HttpStatus.FORBIDDEN,
                post(V2 + "/admin/rotate-kek", Map.of(), payments()).getStatusCode());
        ResponseEntity<Map> health = rest.getForEntity(V2 + "/admin/health", Map.class);
        assertEquals(HttpStatus.OK, health.getStatusCode(), "health stays public on v2 too");
    }

    @Test
    @SuppressWarnings("unchecked")
    void batchCorrelatesByKey() {
        ResponseEntity<Map> resp = post(V2 + "/encrypt/batch", Map.of("items", List.of(
                Map.of("key", "row-1", "plaintext", "a", "dataClassification", "pii"),
                Map.of("key", "row-2", "plaintext", "b"))), payments());
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        List<Map<String, Object>> items = (List<Map<String, Object>>) resp.getBody().get("items");
        assertEquals(List.of("row-1", "row-2"), items.stream().map(i -> i.get("key")).toList());
        items.forEach(i -> assertEquals("success", i.get("status")));
        Map<String, Object> result = (Map<String, Object>) items.get(0).get("result");
        assertTrue(result.containsKey("edekId"), "nested results are camelCase too: " + result.keySet());
    }

    @Test
    void unknownV2PathIs404() {
        ResponseEntity<Map> resp = post(V2 + "/no-such-endpoint", Map.of(), payments());
        assertTrue(resp.getStatusCode() == HttpStatus.NOT_FOUND, "got " + resp.getStatusCode());
    }
}
