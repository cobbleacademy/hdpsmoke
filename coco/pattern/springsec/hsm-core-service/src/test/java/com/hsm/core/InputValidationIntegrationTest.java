package com.hsm.core;

import com.hsm.core.audit.RecentEventsBuffer;
import com.hsm.core.model.EdekRecord;
import com.hsm.core.repository.EdekRecordRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Input validation, size limits, and element mix-up detection. Mirrors
 * tests/integration/test_input_validation.py. The underlying checks
 * (fingerprint cross-check, IV/tag length, AEAD tag verification, plaintext
 * size limit) are already implemented in EncryptRequest/DecryptionService --
 * this class exercises them over the real HTTP layer.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("demo")
class InputValidationIntegrationTest {

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:hsmiv-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private EdekRecordRepository edekRecordRepository;

    @Autowired
    private RecentEventsBuffer recentEvents;

    private static final String TOKEN = "demo-token-payments-svc";
    private static final String APP_ID = "payments-svc";

    private static HttpHeaders headers() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("Authorization", "Bearer " + TOKEN);
        h.set("X-App-ID", APP_ID);
        return h;
    }

    private ResponseEntity<Map> encryptRaw(String plaintext) {
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(Map.of("plaintext", plaintext), headers());
        return rest.postForEntity("/api/sensec/hsm/v1/encrypt", req, Map.class);
    }

    private Map encrypt(String plaintext) {
        ResponseEntity<Map> resp = encryptRaw(plaintext);
        assertEquals(HttpStatus.CREATED, resp.getStatusCode());
        return resp.getBody();
    }

    private ResponseEntity<Map> decryptRaw(Map<String, Object> payload) {
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(payload, headers());
        return rest.postForEntity("/api/sensec/hsm/v1/decrypt", req, Map.class);
    }

    private Map<String, Object> legacyFieldsOf(Map enc) {
        Map<String, Object> m = new HashMap<>();
        m.put("edek_id", enc.get("edek_id"));
        m.put("iv_b64", enc.get("iv_b64"));
        m.put("ciphertext_b64", enc.get("ciphertext_b64"));
        m.put("tag_b64", enc.get("tag_b64"));
        return m;
    }

    private Map<String, Object> lastFailureWithReason(String reason) {
        for (Map<String, Object> ev : recentEvents.recent(100)) {
            if ("failure".equals(ev.get("status")) && reason.equals(ev.get("reason"))) {
                return ev;
            }
        }
        return null;
    }

    // ── Size limits ──────────────────────────────────────────────────────────

    @Test
    void singleBytePlaintextAccepted() {
        assertEquals(HttpStatus.CREATED, encryptRaw("x").getStatusCode());
    }

    @Test
    void sixtyFourKibPlaintextAccepted() {
        String payload = "A".repeat(65_536);
        assertEquals(HttpStatus.CREATED, encryptRaw(payload).getStatusCode());
    }

    @Test
    void over1MibPlaintextRejected() {
        String payload = "A".repeat(1_048_576 + 1);
        ResponseEntity<Map> resp = encryptRaw(payload);
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, resp.getStatusCode());
        String detail = String.valueOf(resp.getBody());
        assertTrue(detail.contains("1048576") || detail.toLowerCase().contains("size"));
    }

    @Test
    void multibyteUnicodeWithinLimitAccepted() {
        // Japanese characters -- 1000 chars total, well within any char or byte limit.
        String payload = "あいうえお".repeat(200);
        assertEquals(HttpStatus.CREATED, encryptRaw(payload).getStatusCode());
    }

    @Test
    void emptyPlaintextRejected() {
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, encryptRaw("").getStatusCode());
    }

    // ── Element integrity ────────────────────────────────────────────────────

    @Test
    void wrongIvLengthRejected() {
        Map enc = encrypt("test");
        Map<String, Object> payload = legacyFieldsOf(enc);
        payload.put("iv_b64", Base64.getEncoder().encodeToString(new byte[8])); // 8 instead of 12
        ResponseEntity<Map> resp = decryptRaw(payload);
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, resp.getStatusCode());
        String detail = String.valueOf(resp.getBody());
        assertTrue(detail.contains("iv_b64"));
        assertTrue(detail.contains("12"));
    }

    @Test
    void wrongTagLengthRejected() {
        Map enc = encrypt("test");
        Map<String, Object> payload = legacyFieldsOf(enc);
        payload.put("tag_b64", Base64.getEncoder().encodeToString(new byte[8])); // 8 instead of 16
        ResponseEntity<Map> resp = decryptRaw(payload);
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, resp.getStatusCode());
        String detail = String.valueOf(resp.getBody());
        assertTrue(detail.contains("tag_b64"));
        assertTrue(detail.contains("16"));
    }

    @Test
    void ivAndTagSwappedBetweenResponsesRejected() {
        Map encA = encrypt("response A");
        Map encB = encrypt("response B");

        Map<String, Object> payload = new HashMap<>();
        payload.put("edek_id", encA.get("edek_id"));
        payload.put("iv_b64", encB.get("iv_b64"));
        payload.put("ciphertext_b64", encA.get("ciphertext_b64"));
        payload.put("tag_b64", encB.get("tag_b64"));

        ResponseEntity<Map> resp = decryptRaw(payload);
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, resp.getStatusCode());
        assertNotNull(lastFailureWithReason("element_mismatch"));
    }

    @Test
    void ciphertextSwappedBetweenResponsesRejected() {
        // iv+tag from A (fingerprint passes), ciphertext from B -> AEAD tag check fails.
        Map encA = encrypt("response A");
        Map encB = encrypt("response B");

        Map<String, Object> payload = legacyFieldsOf(encA);
        payload.put("ciphertext_b64", encB.get("ciphertext_b64"));

        ResponseEntity<Map> resp = decryptRaw(payload);
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, resp.getStatusCode());
        String detail = String.valueOf(resp.getBody()).toLowerCase();
        assertTrue(detail.contains("tampered") || detail.contains("corrupt") || detail.contains("authentication"));
    }

    @Test
    void edekIdFromAEverythingElseFromBRejected() {
        Map encA = encrypt("response A");
        Map encB = encrypt("response B");

        Map<String, Object> payload = legacyFieldsOf(encB);
        payload.put("edek_id", encA.get("edek_id"));

        ResponseEntity<Map> resp = decryptRaw(payload);
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, resp.getStatusCode());
        assertNotNull(lastFailureWithReason("element_mismatch"));
    }

    @Test
    void legacyRecordWithoutFingerprintDecrypts() {
        Map enc = encrypt("legacy record");
        UUID edekId = UUID.fromString((String) enc.get("edek_id"));

        EdekRecord record = edekRecordRepository.findById(edekId).orElseThrow();
        record.setFingerprint(null);
        edekRecordRepository.save(record);

        ResponseEntity<Map> resp = decryptRaw(legacyFieldsOf(enc));
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertEquals("legacy record", resp.getBody().get("plaintext"));
    }

    @Test
    void tamperedCiphertextByteRejected() {
        Map enc = encrypt("tamper me");
        byte[] ct = Base64.getDecoder().decode((String) enc.get("ciphertext_b64"));
        ct[0] ^= 0xFF;

        Map<String, Object> payload = legacyFieldsOf(enc);
        payload.put("ciphertext_b64", Base64.getEncoder().encodeToString(ct));

        ResponseEntity<Map> resp = decryptRaw(payload);
        assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, resp.getStatusCode());
        assertNotNull(lastFailureWithReason("tag_verification_failed"));
    }

    @Test
    void invalidBase64FieldRejected() {
        Map enc = encrypt("base64 test");
        for (String field : List.of("iv_b64", "ciphertext_b64", "tag_b64")) {
            Map<String, Object> payload = legacyFieldsOf(enc);
            payload.put(field, "!!!not-base64!!!");
            ResponseEntity<Map> resp = decryptRaw(payload);
            assertEquals(HttpStatus.UNPROCESSABLE_ENTITY, resp.getStatusCode(), "field=" + field);
        }
    }

    @Test
    void auditEventCarriesSpecificFailureReason() {
        Map encA = encrypt("A");
        Map encB = encrypt("B");

        Map<String, Object> mismatchPayload = legacyFieldsOf(encB);
        mismatchPayload.put("edek_id", encA.get("edek_id"));
        decryptRaw(mismatchPayload);
        assertNotNull(lastFailureWithReason("element_mismatch"));

        Map<String, Object> badIvPayload = legacyFieldsOf(encA);
        badIvPayload.put("iv_b64", Base64.getEncoder().encodeToString(new byte[6]));
        decryptRaw(badIvPayload);
        assertNotNull(lastFailureWithReason("invalid_iv_length"));

        Map<String, Object> badTagPayload = legacyFieldsOf(encA);
        badTagPayload.put("tag_b64", Base64.getEncoder().encodeToString(new byte[4]));
        decryptRaw(badTagPayload);
        assertNotNull(lastFailureWithReason("invalid_tag_length"));
    }
}
