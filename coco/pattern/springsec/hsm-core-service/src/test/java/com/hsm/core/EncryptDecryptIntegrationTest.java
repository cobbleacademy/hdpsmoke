package com.hsm.core;

import com.hsm.core.crypto.DekManager;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
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
@AutoConfigureTestRestTemplate
@ActiveProfiles("demo")
class EncryptDecryptIntegrationTest {

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:hsmit-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private TestRestTemplate rest;

    private static HttpHeaders headers(String token, String appId) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("Authorization", "Bearer " + token);
        h.set("X-App-ID", appId);
        // This suite asserts on the informational/audit fields (edek_id, owner_app_id, ...)
        // that are gated behind X-Response-Detail: full -- see ResponseViews. Requests
        // to non-encrypt/decrypt endpoints (e.g. /admin/grants) just ignore the header.
        h.set("X-Response-Detail", "full");
        return h;
    }

    private String encryptAs(String token, String appId, String plaintext) {
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(Map.of("plaintext", plaintext), headers(token, appId));
        ResponseEntity<Map> resp = rest.postForEntity("/api/sensec/hsm/v1/encrypt", req, Map.class);
        assertEquals(HttpStatus.CREATED, resp.getStatusCode());
        return (String) resp.getBody().get("ciphertext");
    }

    private ResponseEntity<Map> encryptNamed(String token, String appId, String plaintext, String dekName) {
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(
                Map.of("plaintext", plaintext, "dek_name", dekName), headers(token, appId));
        return rest.postForEntity("/api/sensec/hsm/v1/encrypt", req, Map.class);
    }

    @Test
    void encryptThenDecryptSameAppRoundTrips() {
        String ciphertextToken = encryptAs("demo-token-payments-svc", "payments-svc", "top secret");
        assertTrue(ciphertextToken.startsWith("v1."));

        HttpEntity<Map<String, Object>> decReq = new HttpEntity<>(
                Map.of("ciphertext", ciphertextToken), headers("demo-token-payments-svc", "payments-svc"));
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

        // edek_id/iv_b64/ciphertext_b64/tag_b64 no longer come back from /encrypt (see
        // ResponseViews) -- unpack them client-side from the one field every caller
        // gets, same as any real legacy caller would have to do today.
        DekManager.UnpackedToken unpacked = DekManager.unpackToken((String) body.get("ciphertext"));
        HttpEntity<Map<String, Object>> decReq = new HttpEntity<>(Map.of(
                "edek_id", unpacked.edekId().toString(),
                "iv_b64", Base64.getEncoder().encodeToString(unpacked.iv()),
                "ciphertext_b64", Base64.getEncoder().encodeToString(unpacked.ciphertext()),
                "tag_b64", Base64.getEncoder().encodeToString(unpacked.tag())
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
                Map.of("ciphertext", ciphertextToken), headers("demo-token-ops-admin", "ops-admin"));
        ResponseEntity<Map> decResp = rest.postForEntity("/api/sensec/hsm/v1/decrypt", decReq, Map.class);

        assertEquals(HttpStatus.FORBIDDEN, decResp.getStatusCode());
    }

    @Test
    void grantedAppCanDecryptAnotherAppsData() {
        // reporting-app is seeded with a grant to decrypt payments-svc's data.
        String ciphertextToken = encryptAs("demo-token-payments-svc", "payments-svc", "reporting-app should read this");

        HttpEntity<Map<String, Object>> decReq = new HttpEntity<>(
                Map.of("ciphertext", ciphertextToken), headers("demo-token-reporting-app", "reporting-app"));
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
                Map.of("ciphertext", tampered), headers("demo-token-payments-svc", "payments-svc"));
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
        // rotateKek only sweeps KEKs that actually have current EDEKs (see
        // RotationService) -- ensure at least one exists regardless of what other
        // test methods have or haven't run yet, since method order isn't guaranteed.
        encryptAs("demo-token-payments-svc", "payments-svc", "seed a current edek for rotation");

        HttpEntity<Void> req = new HttpEntity<>(headers("demo-token-payments-svc", "payments-svc"));
        ResponseEntity<Map> resp = rest.postForEntity("/api/sensec/hsm/v1/admin/rotate-kek", req, Map.class);
        assertEquals(HttpStatus.FORBIDDEN, resp.getStatusCode());

        HttpEntity<Void> adminReq = new HttpEntity<>(headers("demo-token-ops-admin", "ops-admin"));
        ResponseEntity<Map> adminResp = rest.postForEntity("/api/sensec/hsm/v1/admin/rotate-kek", adminReq, Map.class);
        assertEquals(HttpStatus.OK, adminResp.getStatusCode());
        List<Map> results = (List<Map>) adminResp.getBody().get("results");
        assertFalse(results.isEmpty());
        assertNotNull(results.get(0).get("new_kek_version"));
    }

    @Test
    void grantsCanBeAddedListedAndRemoved() {
        HttpHeaders adminHeaders = headers("demo-token-ops-admin", "ops-admin");

        HttpEntity<Map<String, Object>> addReq = new HttpEntity<>(
                Map.of("grantee_app_id", "ops-admin", "owner_app_id", "reporting-app", "scope", "decrypt"), adminHeaders);
        ResponseEntity<Map> addResp = rest.postForEntity("/api/sensec/hsm/v1/admin/grants", addReq, Map.class);
        assertEquals(HttpStatus.CREATED, addResp.getStatusCode());
        assertNotNull(addResp.getBody().get("created_at"));

        ResponseEntity<Map> listResp = rest.exchange("/api/sensec/hsm/v1/admin/grants", HttpMethod.GET,
                new HttpEntity<>(adminHeaders), Map.class);
        assertEquals(HttpStatus.OK, listResp.getStatusCode());
        List<Map> grants = (List<Map>) listResp.getBody().get("grants");
        assertTrue(grants.stream().anyMatch(g ->
                "ops-admin".equals(g.get("grantee_app_id")) && g.get("created_at") != null));

        HttpEntity<Map<String, Object>> removeReq = new HttpEntity<>(
                Map.of("grantee_app_id", "ops-admin", "owner_app_id", "reporting-app", "scope", "decrypt"), adminHeaders);
        ResponseEntity<Void> removeResp = rest.exchange("/api/sensec/hsm/v1/admin/grants", HttpMethod.DELETE, removeReq, Void.class);
        assertEquals(HttpStatus.NO_CONTENT, removeResp.getStatusCode());
    }

    @Test
    void grantWithUnknownScopeIsRejected() {
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(
                Map.of("grantee_app_id", "ops-admin", "owner_app_id", "reporting-app", "scope", "bogus-scope"),
                headers("demo-token-ops-admin", "ops-admin"));
        ResponseEntity<Map> resp = rest.postForEntity("/api/sensec/hsm/v1/admin/grants", req, Map.class);
        assertEquals(HttpStatus.UNPROCESSABLE_CONTENT, resp.getStatusCode());
    }

    @Test
    void secondAppReusingAnotherAppsDekNameWithoutGrantIsForbidden() {
        // payments-svc mints "cross.app.dek.a" first and becomes its owner.
        ResponseEntity<Map> first = encryptNamed(
                "demo-token-payments-svc", "payments-svc", "owned by payments-svc", "cross.app.dek.a");
        assertEquals(HttpStatus.CREATED, first.getStatusCode());

        // ops-admin also has "encrypt" scope but no grant on payments-svc's DEKs --
        // reusing the same dek_name must be rejected, not silently mint its own DEK.
        ResponseEntity<Map> second = encryptNamed(
                "demo-token-ops-admin", "ops-admin", "should not reuse", "cross.app.dek.a");
        assertEquals(HttpStatus.FORBIDDEN, second.getStatusCode());
    }

    @Test
    void coarseEncryptGrantAllowsReusingAnotherAppsDekName() {
        ResponseEntity<Map> first = encryptNamed(
                "demo-token-payments-svc", "payments-svc", "owned by payments-svc", "cross.app.dek.b");
        assertEquals(HttpStatus.CREATED, first.getStatusCode());

        HttpEntity<Map<String, Object>> grantReq = new HttpEntity<>(
                Map.of("grantee_app_id", "ops-admin", "owner_app_id", "payments-svc", "scope", "encrypt"),
                headers("demo-token-ops-admin", "ops-admin"));
        ResponseEntity<Map> grantResp = rest.postForEntity("/api/sensec/hsm/v1/admin/grants", grantReq, Map.class);
        assertEquals(HttpStatus.CREATED, grantResp.getStatusCode());

        ResponseEntity<Map> second = encryptNamed(
                "demo-token-ops-admin", "ops-admin", "now allowed to reuse", "cross.app.dek.b");
        assertEquals(HttpStatus.CREATED, second.getStatusCode());
        assertEquals(first.getBody().get("edek_id"), second.getBody().get("edek_id"));

        HttpEntity<Map<String, Object>> removeReq = new HttpEntity<>(
                Map.of("grantee_app_id", "ops-admin", "owner_app_id", "payments-svc", "scope", "encrypt"),
                headers("demo-token-ops-admin", "ops-admin"));
        rest.exchange("/api/sensec/hsm/v1/admin/grants", HttpMethod.DELETE, removeReq, Void.class);
    }

    @Test
    void fineGrainedDekGrantAllowsOnlyThatSpecificDekName() {
        ResponseEntity<Map> ownerRecord = encryptNamed(
                "demo-token-payments-svc", "payments-svc", "fine grained target", "cross.app.dek.c");
        assertEquals(HttpStatus.CREATED, ownerRecord.getStatusCode());
        ResponseEntity<Map> otherOwnerRecord = encryptNamed(
                "demo-token-payments-svc", "payments-svc", "not covered by the grant", "cross.app.dek.d");
        assertEquals(HttpStatus.CREATED, otherOwnerRecord.getStatusCode());

        HttpHeaders adminHeaders = headers("demo-token-ops-admin", "ops-admin");
        HttpEntity<Map<String, Object>> dekGrantReq = new HttpEntity<>(
                Map.of("grantee_app_id", "ops-admin", "owner_app_id", "payments-svc",
                        "dek_name", "cross.app.dek.c", "scope", "encrypt"),
                adminHeaders);
        ResponseEntity<Map> dekGrantResp = rest.postForEntity("/api/sensec/hsm/v1/admin/dek-grants", dekGrantReq, Map.class);
        assertEquals(HttpStatus.CREATED, dekGrantResp.getStatusCode());

        ResponseEntity<Map> listResp = rest.exchange("/api/sensec/hsm/v1/admin/dek-grants", HttpMethod.GET,
                new HttpEntity<>(adminHeaders), Map.class);
        assertEquals(HttpStatus.OK, listResp.getStatusCode());
        List<Map> dekGrants = (List<Map>) listResp.getBody().get("grants");
        assertTrue(dekGrants.stream().anyMatch(g -> "cross.app.dek.c".equals(g.get("dek_name"))));

        // Covered by the fine-grained grant.
        ResponseEntity<Map> allowed = encryptNamed(
                "demo-token-ops-admin", "ops-admin", "reuse via dek grant", "cross.app.dek.c");
        assertEquals(HttpStatus.CREATED, allowed.getStatusCode());
        assertEquals(ownerRecord.getBody().get("edek_id"), allowed.getBody().get("edek_id"));

        // Not covered -- the grant is scoped to one dek_name only.
        ResponseEntity<Map> denied = encryptNamed(
                "demo-token-ops-admin", "ops-admin", "still not covered", "cross.app.dek.d");
        assertEquals(HttpStatus.FORBIDDEN, denied.getStatusCode());

        HttpEntity<Map<String, Object>> removeReq = new HttpEntity<>(
                Map.of("grantee_app_id", "ops-admin", "owner_app_id", "payments-svc",
                        "dek_name", "cross.app.dek.c", "scope", "encrypt"),
                adminHeaders);
        ResponseEntity<Void> removeResp = rest.exchange("/api/sensec/hsm/v1/admin/dek-grants", HttpMethod.DELETE, removeReq, Void.class);
        assertEquals(HttpStatus.NO_CONTENT, removeResp.getStatusCode());
    }
}
