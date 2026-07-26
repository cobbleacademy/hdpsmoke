package com.hsm.encryption;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end coverage over the real HTTP layer (auth resolver, scope checks,
 * grant model, crypto round trip), running under the demo profile against an
 * isolated in-memory H2 instance (not the file-based demo DB used for manual
 * verification). Mirrors tests/integration/test_encrypt_decrypt.py,
 * test_pbac_decisions.py (implicitly, via NullPbacClient in demo mode), and
 * test_ciphertext_token.py.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("demo")
class EncryptDecryptIntegrationTest {

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:hsmit-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1");
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

    private String encryptAs(String token, String appId, String plaintext) {
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(Map.of("plaintext", plaintext), headers(token, appId));
        ResponseEntity<Map> resp = rest.postForEntity("/api/sensec/hsm/v1/encrypt", req, Map.class);
        assertEquals(HttpStatus.CREATED, resp.getStatusCode());
        return (String) resp.getBody().get("ciphertext_token");
    }

    @Test
    void encryptThenDecryptSameAppRoundTrips() {
        String ciphertextToken = encryptAs("demo-token-payments-svc", "payments-svc", "top secret");
        assertTrue(ciphertextToken.startsWith("v1."));

        HttpEntity<Map<String, Object>> decReq = new HttpEntity<>(
                Map.of("ciphertext_token", ciphertextToken), headers("demo-token-payments-svc", "payments-svc"));
        ResponseEntity<Map> decResp = rest.postForEntity("/api/sensec/hsm/v1/decrypt", decReq, Map.class);

        assertEquals(HttpStatus.OK, decResp.getStatusCode());
        assertEquals("top secret", decResp.getBody().get("plaintext"));
        assertEquals("payments-svc", decResp.getBody().get("owner_app_id"));
    }

    @Test
    void legacyFieldsRoundTrip() {
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(
                Map.of("plaintext", "legacy path"), headers("demo-token-payments-svc", "payments-svc"));
        ResponseEntity<Map> encResp = rest.postForEntity("/api/sensec/hsm/v1/encrypt", req, Map.class);
        Map body = encResp.getBody();

        HttpEntity<Map<String, Object>> decReq = new HttpEntity<>(Map.of(
                "edek_id", body.get("edek_id"),
                "iv_b64", body.get("iv_b64"),
                "ciphertext_b64", body.get("ciphertext_b64"),
                "tag_b64", body.get("tag_b64")
        ), headers("demo-token-payments-svc", "payments-svc"));
        ResponseEntity<Map> decResp = rest.postForEntity("/api/sensec/hsm/v1/decrypt", decReq, Map.class);

        assertEquals(HttpStatus.OK, decResp.getStatusCode());
        assertEquals("legacy path", decResp.getBody().get("plaintext"));
    }

    @Test
    void crossAppDecryptDeniedWithoutGrant() {
        // ops-admin has no seeded grant to read payments-svc's data.
        String ciphertextToken = encryptAs("demo-token-payments-svc", "payments-svc", "cross-app secret");

        HttpEntity<Map<String, Object>> decReq = new HttpEntity<>(
                Map.of("ciphertext_token", ciphertextToken), headers("demo-token-ops-admin", "ops-admin"));
        ResponseEntity<Map> decResp = rest.postForEntity("/api/sensec/hsm/v1/decrypt", decReq, Map.class);

        assertEquals(HttpStatus.FORBIDDEN, decResp.getStatusCode());
    }

    @Test
    void grantedAppCanDecryptAnotherAppsData() {
        // reporting-app is seeded with a grant to decrypt payments-svc's data.
        String ciphertextToken = encryptAs("demo-token-payments-svc", "payments-svc", "reporting-app should read this");

        HttpEntity<Map<String, Object>> decReq = new HttpEntity<>(
                Map.of("ciphertext_token", ciphertextToken), headers("demo-token-reporting-app", "reporting-app"));
        ResponseEntity<Map> decResp = rest.postForEntity("/api/sensec/hsm/v1/decrypt", decReq, Map.class);

        assertEquals(HttpStatus.OK, decResp.getStatusCode());
        assertEquals("reporting-app should read this", decResp.getBody().get("plaintext"));
    }

    @Test
    void scopeEnforcedOnEncryptEndpoint() {
        // reporting-app only has "decrypt" scope, not "encrypt".
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(
                Map.of("plaintext", "should be denied"), headers("demo-token-reporting-app", "reporting-app"));
        ResponseEntity<Map> resp = rest.postForEntity("/api/sensec/hsm/v1/encrypt", req, Map.class);
        assertEquals(HttpStatus.FORBIDDEN, resp.getStatusCode());
    }

    @Test
    void missingBearerTokenIsRejected() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("X-App-ID", "payments-svc");
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(Map.of("plaintext", "x"), h);
        ResponseEntity<Map> resp = rest.postForEntity("/api/sensec/hsm/v1/encrypt", req, Map.class);
        assertEquals(HttpStatus.UNAUTHORIZED, resp.getStatusCode());
    }

    @Test
    void appIdHeaderMismatchIsRejected() {
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(
                Map.of("plaintext", "x"), headers("demo-token-payments-svc", "reporting-app"));
        ResponseEntity<Map> resp = rest.postForEntity("/api/sensec/hsm/v1/encrypt", req, Map.class);
        assertEquals(HttpStatus.FORBIDDEN, resp.getStatusCode());
    }

    @Test
    void tamperedCiphertextTokenFailsAuthentication() {
        String ciphertextToken = encryptAs("demo-token-payments-svc", "payments-svc", "tamper me");

        char[] chars = ciphertextToken.toCharArray();
        int mutateIdx = chars.length - 5; // safely within the ciphertext, not the trailing padding
        chars[mutateIdx] = chars[mutateIdx] == 'A' ? 'B' : 'A';
        String tampered = new String(chars);

        HttpEntity<Map<String, Object>> decReq = new HttpEntity<>(
                Map.of("ciphertext_token", tampered), headers("demo-token-payments-svc", "payments-svc"));
        ResponseEntity<Map> decResp = rest.postForEntity("/api/sensec/hsm/v1/decrypt", decReq, Map.class);

        assertTrue(decResp.getStatusCode().is4xxClientError());
    }

    @Test
    void healthEndpointIsPublicAndReportsOk() {
        ResponseEntity<Map> resp = rest.getForEntity("/api/sensec/hsm/v1/admin/health", Map.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        assertEquals("ok", resp.getBody().get("status"));
        assertEquals(Boolean.TRUE, resp.getBody().get("db_reachable"));
    }

    @Test
    void rotateKekRequiresRotateScope() {
        HttpEntity<Void> req = new HttpEntity<>(headers("demo-token-payments-svc", "payments-svc"));
        ResponseEntity<Map> resp = rest.postForEntity("/api/sensec/hsm/v1/admin/rotate-kek", req, Map.class);
        assertEquals(HttpStatus.FORBIDDEN, resp.getStatusCode());

        HttpEntity<Void> adminReq = new HttpEntity<>(headers("demo-token-ops-admin", "ops-admin"));
        ResponseEntity<Map> adminResp = rest.postForEntity("/api/sensec/hsm/v1/admin/rotate-kek", adminReq, Map.class);
        assertEquals(HttpStatus.OK, adminResp.getStatusCode());
        assertNotNull(adminResp.getBody().get("new_kek_version"));
    }

    @Test
    void grantsCanBeAddedListedAndRemoved() {
        HttpHeaders adminHeaders = headers("demo-token-ops-admin", "ops-admin");

        HttpEntity<Map<String, Object>> addReq = new HttpEntity<>(
                Map.of("grantee_app_id", "ops-admin", "owner_app_id", "reporting-app"), adminHeaders);
        ResponseEntity<Map> addResp = rest.postForEntity("/api/sensec/hsm/v1/admin/grants", addReq, Map.class);
        assertEquals(HttpStatus.CREATED, addResp.getStatusCode());

        ResponseEntity<Map> listResp = rest.exchange("/api/sensec/hsm/v1/admin/grants", HttpMethod.GET,
                new HttpEntity<>(adminHeaders), Map.class);
        assertEquals(HttpStatus.OK, listResp.getStatusCode());

        HttpEntity<Map<String, Object>> removeReq = new HttpEntity<>(
                Map.of("grantee_app_id", "ops-admin", "owner_app_id", "reporting-app"), adminHeaders);
        ResponseEntity<Void> removeResp = rest.exchange("/api/sensec/hsm/v1/admin/grants", HttpMethod.DELETE, removeReq, Void.class);
        assertEquals(HttpStatus.NO_CONTENT, removeResp.getStatusCode());
    }
}
