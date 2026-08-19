package com.hsm.core;

import com.hsm.core.crypto.DekManager;
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
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * POST /decrypt/batch -- symmetric to BatchEncryptIntegrationTest. See
 * java/docs/BULK_OPERATIONS.md.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("demo")
class BatchDecryptIntegrationTest {

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:hsmbdec-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private TestRestTemplate rest;

    private static final String TOKEN = "demo-token-payments-svc";
    private static final String APP_ID = "payments-svc";

    private static HttpHeaders headers() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("Authorization", "Bearer " + TOKEN);
        h.set("X-App-ID", APP_ID);
        return h;
    }

    private String encryptOne(String plaintext) {
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(Map.of("plaintext", plaintext), headers());
        ResponseEntity<Map> resp = rest.postForEntity("/api/sensec/hsm/v1/encrypt", req, Map.class);
        assertEquals(HttpStatus.CREATED, resp.getStatusCode());
        return (String) resp.getBody().get("ciphertext");
    }

    private ResponseEntity<Map> postBatchDecrypt(List<Map<String, Object>> items) {
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(Map.of("items", items), headers());
        return rest.postForEntity("/api/sensec/hsm/v1/decrypt/batch", req, Map.class);
    }

    private static Map<String, Object> item(String key, String ciphertext) {
        return Map.of("key", key, "ciphertext", ciphertext);
    }

    @Test
    void batchDecryptsMultipleItemsCorrelatedByKey() {
        String tokenA = encryptOne("first secret");
        String tokenB = encryptOne("second secret");

        ResponseEntity<Map> resp = postBatchDecrypt(List.of(
                item("row-1", tokenA),
                item("row-2", tokenB)
        ));

        assertEquals(HttpStatus.OK, resp.getStatusCode());
        List<Map> items = (List<Map>) resp.getBody().get("items");
        assertEquals(2, items.size());

        Map<String, Map> byKey = new HashMap<>();
        for (Map item : items) {
            byKey.put((String) item.get("key"), item);
        }
        assertEquals("success", byKey.get("row-1").get("status"));
        assertEquals("first secret", ((Map) byKey.get("row-1").get("result")).get("plaintext"));
        assertEquals("success", byKey.get("row-2").get("status"));
        assertEquals("second secret", ((Map) byKey.get("row-2").get("result")).get("plaintext"));
    }

    @Test
    void batchDecryptLegacyFieldsWork() {
        HttpEntity<Map<String, Object>> encReq = new HttpEntity<>(Map.of("plaintext", "legacy path"), headers());
        Map enc = rest.postForEntity("/api/sensec/hsm/v1/encrypt", encReq, Map.class).getBody();

        // edek_id/iv_b64/ciphertext_b64/tag_b64 no longer come back from /encrypt (see
        // ResponseViews) -- unpack them client-side from the one field every caller
        // gets, same as any real legacy caller would have to do today.
        DekManager.UnpackedToken unpacked = DekManager.unpackToken((String) enc.get("ciphertext"));
        Map<String, Object> item = new HashMap<>();
        item.put("key", "legacy-item");
        item.put("edek_id", unpacked.edekId().toString());
        item.put("iv_b64", Base64.getEncoder().encodeToString(unpacked.iv()));
        item.put("ciphertext_b64", Base64.getEncoder().encodeToString(unpacked.ciphertext()));
        item.put("tag_b64", Base64.getEncoder().encodeToString(unpacked.tag()));

        ResponseEntity<Map> resp = postBatchDecrypt(List.of(item));
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        Map result = (Map) ((List<Map>) resp.getBody().get("items")).get(0);
        assertEquals("success", result.get("status"));
        assertEquals("legacy path", ((Map) result.get("result")).get("plaintext"));
    }

    @Test
    void oneMalformedItemDoesNotFailWholeBatch() {
        String goodToken = encryptOne("good item");

        ResponseEntity<Map> resp = postBatchDecrypt(List.of(
                item("good", goodToken),
                item("bad", "v1.not-a-real-token")
        ));

        // Malformed token is a per-item outcome (checked inside decrypt()), not a
        // structural/Bean-Validation violation -- so the batch itself still succeeds (200).
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        Map<String, Map> byKey = new HashMap<>();
        for (Map item : (List<Map>) resp.getBody().get("items")) {
            byKey.put((String) item.get("key"), item);
        }
        assertEquals("success", byKey.get("good").get("status"));
        assertEquals("good item", ((Map) byKey.get("good").get("result")).get("plaintext"));
        assertEquals("error", byKey.get("bad").get("status"));
    }

    @Test
    void duplicateKeyRejectsWholeBatch() {
        String token = encryptOne("x");
        ResponseEntity<Map> resp = postBatchDecrypt(List.of(item("dup", token), item("dup", token)));
        assertEquals(HttpStatus.UNPROCESSABLE_CONTENT, resp.getStatusCode());
        assertTrue(String.valueOf(resp.getBody()).contains("duplicate"));
    }

    @Test
    void emptyBatchRejected() {
        assertEquals(HttpStatus.UNPROCESSABLE_CONTENT, postBatchDecrypt(List.of()).getStatusCode());
    }

    @Test
    void batchExceedingMaxItemCountRejected() {
        String token = encryptOne("x");
        List<Map<String, Object>> items = new ArrayList<>();
        for (int i = 0; i < 101; i++) {
            items.add(item("key-" + i, token));
        }
        ResponseEntity<Map> resp = postBatchDecrypt(items);
        assertEquals(HttpStatus.UNPROCESSABLE_CONTENT, resp.getStatusCode());
        assertTrue(String.valueOf(resp.getBody()).contains("maximum item count"));
    }

    @Test
    void crossAppDecryptDeniedPerItemWithoutGrant() {
        // ops-admin has no seeded grant to read payments-svc's data.
        String token = encryptOne("owned by payments-svc");
        HttpHeaders opsHeaders = new HttpHeaders();
        opsHeaders.setContentType(MediaType.APPLICATION_JSON);
        opsHeaders.set("Authorization", "Bearer demo-token-ops-admin");
        opsHeaders.set("X-App-ID", "ops-admin");

        HttpEntity<Map<String, Object>> req = new HttpEntity<>(Map.of("items", List.of(item("k", token))), opsHeaders);
        ResponseEntity<Map> resp = rest.postForEntity("/api/sensec/hsm/v1/decrypt/batch", req, Map.class);

        assertEquals(HttpStatus.OK, resp.getStatusCode());
        Map result = (Map) ((List<Map>) resp.getBody().get("items")).get(0);
        assertEquals("error", result.get("status"));
    }

    @Test
    void batchDecryptRequiresAuthentication() {
        HttpHeaders noAuth = new HttpHeaders();
        noAuth.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(Map.of("items", List.of(item("k", "v1.x"))), noAuth);
        ResponseEntity<Map> resp = rest.postForEntity("/api/sensec/hsm/v1/decrypt/batch", req, Map.class);
        assertEquals(HttpStatus.UNAUTHORIZED, resp.getStatusCode());
    }
}
