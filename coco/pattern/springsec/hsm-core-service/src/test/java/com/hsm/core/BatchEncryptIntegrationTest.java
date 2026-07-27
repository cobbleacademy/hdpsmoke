package com.hsm.core;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * POST /encrypt/batch -- multiple plaintexts, one authenticated call, each
 * correlated back by a caller-supplied key. See java/docs/BULK_OPERATIONS.md.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("demo")
class BatchEncryptIntegrationTest {

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:hsmbatch-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private TestRestTemplate rest;

    private static HttpHeaders headers(String token, String appId) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("Authorization", "Bearer " + token);
        h.set("X-App-ID", appId);
        return h;
    }

    private static Map<String, Object> item(String key, String plaintext) {
        return Map.of("key", key, "plaintext", plaintext);
    }

    private ResponseEntity<Map> postBatch(List<Map<String, Object>> items) {
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(
                Map.of("items", items), headers("demo-token-payments-svc", "payments-svc"));
        return rest.postForEntity("/api/sensec/hsm/v1/encrypt/batch", req, Map.class);
    }

    @Test
    void batchEncryptsMultipleItemsCorrelatedByKey() {
        ResponseEntity<Map> resp = postBatch(List.of(
                item("row-1", "first secret"),
                item("row-2", "second secret"),
                item("row-3", "third secret")
        ));

        assertEquals(HttpStatus.OK, resp.getStatusCode());
        List<Map> items = (List<Map>) resp.getBody().get("items");
        assertEquals(3, items.size());

        Map<String, Map> byKey = new java.util.HashMap<>();
        for (Map item : items) {
            byKey.put((String) item.get("key"), item);
        }
        assertEquals("success", byKey.get("row-1").get("status"));
        assertEquals("success", byKey.get("row-2").get("status"));
        assertEquals("success", byKey.get("row-3").get("status"));

        Map row1Result = (Map) byKey.get("row-1").get("result");
        assertNotNull(row1Result.get("ciphertext_token"));
        assertTrue(((String) row1Result.get("ciphertext_token")).startsWith("v1."));
    }

    @Test
    void batchResultDecryptsToOriginalPlaintext() {
        ResponseEntity<Map> resp = postBatch(List.of(item("only-item", "round trip me")));
        Map result = (Map) ((List<Map>) resp.getBody().get("items")).get(0).get("result");
        String token = (String) result.get("ciphertext_token");

        HttpEntity<Map<String, Object>> decReq = new HttpEntity<>(
                Map.of("ciphertext_token", token), headers("demo-token-payments-svc", "payments-svc"));
        ResponseEntity<Map> decResp = rest.postForEntity("/api/sensec/hsm/v1/decrypt", decReq, Map.class);

        assertEquals(HttpStatus.OK, decResp.getStatusCode());
        assertEquals("round trip me", decResp.getBody().get("plaintext"));
    }

    @Test
    void duplicateKeyRejectsWholeBatch() {
        ResponseEntity<Map> resp = postBatch(List.of(
                item("dup", "first"),
                item("dup", "second")
        ));
        assertEquals(HttpStatus.UNPROCESSABLE_CONTENT, resp.getStatusCode());
        String detail = String.valueOf(resp.getBody());
        assertTrue(detail.contains("duplicate"));
    }

    @Test
    void emptyBatchRejected() {
        ResponseEntity<Map> resp = postBatch(List.of());
        assertEquals(HttpStatus.UNPROCESSABLE_CONTENT, resp.getStatusCode());
    }

    @Test
    void batchExceedingMaxItemCountRejected() {
        List<Map<String, Object>> items = new ArrayList<>();
        for (int i = 0; i < 101; i++) {
            items.add(item("key-" + i, "value-" + i));
        }
        ResponseEntity<Map> resp = postBatch(items);
        assertEquals(HttpStatus.UNPROCESSABLE_CONTENT, resp.getStatusCode());
        String detail = String.valueOf(resp.getBody());
        assertTrue(detail.contains("maximum item count"));
    }

    @Test
    void blankPlaintextInOneItemRejectsWholeBatch() {
        ResponseEntity<Map> resp = postBatch(List.of(
                item("good", "fine"),
                item("bad", "")
        ));
        assertEquals(HttpStatus.UNPROCESSABLE_CONTENT, resp.getStatusCode());
    }

    @Test
    void batchRequiresEncryptScope() {
        // reporting-app only has "decrypt", not "encrypt".
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(
                Map.of("items", List.of(item("k", "v"))), headers("demo-token-reporting-app", "reporting-app"));
        ResponseEntity<Map> resp = rest.postForEntity("/api/sensec/hsm/v1/encrypt/batch", req, Map.class);
        assertEquals(HttpStatus.FORBIDDEN, resp.getStatusCode());
    }
}
