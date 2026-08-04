package com.hsm.core;

import com.hsm.core.model.EdekRecord;
import com.hsm.core.model.RotationStatus;
import com.hsm.core.repository.EdekRecordRepository;
import com.hsm.core.service.RotationService;
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

import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Covers dek_name reuse/mint on /encrypt, the classification-immutability rule, and RotationService.rotateNamedDeks. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("demo")
class NamedDekIntegrationTest {

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:hsmnameddek-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private EdekRecordRepository edekRecordRepository;

    @Autowired
    private RotationService rotationService;

    private static HttpHeaders headers(String token, String appId) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("Authorization", "Bearer " + token);
        h.set("X-App-ID", appId);
        return h;
    }

    private Map encryptAs(String plaintext, String dekName, String dataClassification) {
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(
                Map.of("plaintext", plaintext, "dek_name", dekName == null ? "" : dekName,
                        "data_classification", dataClassification == null ? "" : dataClassification),
                headers("demo-token-payments-svc", "payments-svc"));
        ResponseEntity<Map> resp = rest.postForEntity("/api/sensec/hsm/v1/encrypt", req, Map.class);
        assertEquals(HttpStatus.CREATED, resp.getStatusCode());
        return resp.getBody();
    }

    @Test
    void secondCallForSameNameReusesEdekIdAndDecryptsCorrectly() {
        Map first = encryptAs("value one", "test.reuse.column", "pii");
        assertEquals(Boolean.FALSE, first.get("reused"));

        Map second = encryptAs("value two", "test.reuse.column", "pii");
        assertEquals(Boolean.TRUE, second.get("reused"));
        assertEquals(first.get("edek_id"), second.get("edek_id"));

        // Both tokens still decrypt independently and correctly despite sharing a DEK.
        for (Map.Entry<String, String> e : Map.of((String) first.get("ciphertext_token"), "value one",
                (String) second.get("ciphertext_token"), "value two").entrySet()) {
            HttpEntity<Map<String, Object>> decReq = new HttpEntity<>(
                    Map.of("ciphertext_token", e.getKey()), headers("demo-token-payments-svc", "payments-svc"));
            ResponseEntity<Map> decResp = rest.postForEntity("/api/sensec/hsm/v1/decrypt", decReq, Map.class);
            assertEquals(HttpStatus.OK, decResp.getStatusCode());
            assertEquals(e.getValue(), decResp.getBody().get("plaintext"));
        }
    }

    @Test
    void withoutDekNameEveryCallStillMintsItsOwn() {
        Map first = encryptAs("independent one", null, null);
        Map second = encryptAs("independent two", null, null);
        assertEquals(Boolean.FALSE, first.get("reused"));
        assertEquals(Boolean.FALSE, second.get("reused"));
        assertNotEquals(first.get("edek_id"), second.get("edek_id"));
    }

    @Test
    void conflictingClassificationOnExistingNameRejected() {
        encryptAs("first", "test.classification.column", "pii");

        HttpEntity<Map<String, Object>> req = new HttpEntity<>(
                Map.of("plaintext", "second", "dek_name", "test.classification.column", "data_classification", "pci"),
                headers("demo-token-payments-svc", "payments-svc"));
        ResponseEntity<Map> resp = rest.postForEntity("/api/sensec/hsm/v1/encrypt", req, Map.class);
        assertEquals(HttpStatus.UNPROCESSABLE_CONTENT, resp.getStatusCode());
    }

    @Test
    void blankClassificationOnSubsequentCallDoesNotConflict() {
        Map first = encryptAs("first", "test.blank.column", "pii");
        Map second = encryptAs("second", "test.blank.column", null);
        assertEquals(Boolean.TRUE, second.get("reused"));
        assertEquals(first.get("edek_id"), second.get("edek_id"));
    }

    @Test
    void rotateNamedDeksMintsFreshRowAndRetiresOld() {
        Map first = encryptAs("rotation subject", "test.rotation.column", "pii");
        UUID oldEdekId = UUID.fromString((String) first.get("edek_id"));

        EdekRecord old = edekRecordRepository.findById(oldEdekId).orElseThrow();
        old.setRotationStatus(RotationStatus.CURRENT);
        // Force it to look old enough to rotate without waiting on a real clock.
        java.lang.reflect.Field createdAtField;
        try {
            createdAtField = EdekRecord.class.getDeclaredField("createdAt");
            createdAtField.setAccessible(true);
            createdAtField.set(old, OffsetDateTime.now().minusHours(1));
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        edekRecordRepository.save(old);

        int rotated = rotationService.rotateNamedDeks(0);
        assertTrue(rotated >= 1);

        EdekRecord reloadedOld = edekRecordRepository.findById(oldEdekId).orElseThrow();
        assertEquals(RotationStatus.ROTATED, reloadedOld.getRotationStatus());
        assertNull(reloadedOld.getCurrentDekName());
        assertEquals("test.rotation.column", reloadedOld.getDekName()); // history preserved

        Optional<EdekRecord> fresh = edekRecordRepository.findByAppIdAndCurrentDekName("payments-svc", "test.rotation.column");
        assertTrue(fresh.isPresent());
        assertNotEquals(oldEdekId, fresh.get().getEdekId());
        assertEquals("pii", fresh.get().getDataClassification());

        // The old token still decrypts (unwrap doesn't care about rotation_status),
        // and a fresh /encrypt call for the same name now resolves to the new row.
        HttpEntity<Map<String, Object>> decReq = new HttpEntity<>(
                Map.of("ciphertext_token", first.get("ciphertext_token")), headers("demo-token-payments-svc", "payments-svc"));
        ResponseEntity<Map> decResp = rest.postForEntity("/api/sensec/hsm/v1/decrypt", decReq, Map.class);
        assertEquals(HttpStatus.OK, decResp.getStatusCode());
        assertEquals("rotation subject", decResp.getBody().get("plaintext"));

        Map afterRotation = encryptAs("post rotation", "test.rotation.column", "pii");
        assertEquals(Boolean.TRUE, afterRotation.get("reused"));
        assertEquals(fresh.get().getEdekId().toString(), afterRotation.get("edek_id"));
    }
}
