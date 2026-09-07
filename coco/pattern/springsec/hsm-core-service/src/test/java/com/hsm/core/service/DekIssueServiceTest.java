package com.hsm.core.service;

import com.hsm.core.crypto.DekManager;
import com.hsm.core.crypto.KekClient;
import com.hsm.core.crypto.TransportWrapper;
import com.hsm.core.dto.DekIssueItem;
import com.hsm.core.dto.DekIssueRequest;
import com.hsm.core.dto.DekIssueResponse;
import com.hsm.core.dto.DekIssueResultItem;
import com.hsm.core.model.AppGrant;
import com.hsm.core.model.AppRegistration;
import com.hsm.core.model.EdekRecord;
import com.hsm.core.repository.AppGrantRepository;
import com.hsm.core.repository.AppRegistrationRepository;
import com.hsm.core.repository.EdekRecordRepository;
import com.hsm.core.web.ApiException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("demo")
class DekIssueServiceTest {

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:dekissue-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private DekIssueService dekIssueService;

    @Autowired
    private AppRegistrationRepository appRegistrationRepository;

    @Autowired
    private EdekRecordRepository edekRecordRepository;

    @Autowired
    private AppGrantRepository appGrantRepository;

    @Autowired
    private KekClient kekClient;

    private static KeyPair generateTestKeyPair() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        return gen.generateKeyPair();
    }

    private static String pem(byte[] der, String label) {
        return "-----BEGIN " + label + "-----\n" + Base64.getEncoder().encodeToString(der) + "\n-----END " + label + "-----\n";
    }

    private String registerAppWithKeyPair(String appId, PublicKey publicKey) {
        String pem = pem(publicKey.getEncoded(), "PUBLIC KEY");
        AppRegistration registration = new AppRegistration(appId, "dek_issue,dek_unwrap", "test app", true);
        registration.setPublicKeyPem(pem);
        appRegistrationRepository.save(registration);
        return pem;
    }

    @Test
    void issuedDekPersistsAsPlainEdekRecordAndTransportUnwrapsToKekWrappedValue() throws Exception {
        KeyPair keyPair = generateTestKeyPair();
        String appId = "bulk-test-app-1";
        registerAppWithKeyPair(appId, keyPair.getPublic());

        DekIssueRequest request = new DekIssueRequest(List.of(new DekIssueItem("row-1", "pii", null)));
        DekIssueResponse response = dekIssueService.issue(request, appId, "test-sub", "127.0.0.1");

        assertEquals(1, response.items().size());
        DekIssueResultItem item = response.items().get(0);
        assertEquals("success", item.status());
        assertEquals("row-1", item.key());
        assertNotNull(item.edekId());
        assertNotNull(item.wrappedDekB64());
        assertFalse(item.reused());

        // Persisted shape matches what EncryptionService.encrypt would write, minus the AES-GCM fields.
        Optional<EdekRecord> maybeRecord = edekRecordRepository.findById(item.edekId());
        assertTrue(maybeRecord.isPresent());
        EdekRecord record = maybeRecord.get();
        assertEquals(appId, record.getAppId());
        assertEquals(DekManager.ALGORITHM, record.getAlgorithm());
        assertEquals("utf8", record.getEncoding());
        assertEquals("pii", record.getDataClassification());
        assertEquals(com.hsm.core.model.RotationStatus.CURRENT, record.getRotationStatus());
        assertNull(record.getFingerprint());
        assertNotNull(record.getKekName());

        // Round-trip: unwrap the KEK-wrapped EDEK (as /decrypt would) and separately RSA-unwrap the
        // transport wrap (as CLNT would) -- both must yield the exact same raw DEK bytes.
        byte[] edekBytes = Base64.getDecoder().decode(record.getEdekBlob());
        byte[] dekViaKek = kekClient.unwrapDek(edekBytes, record.getKekName(), record.getKekVersion());

        byte[] wrappedForTransport = Base64.getDecoder().decode(item.wrappedDekB64());
        byte[] dekViaTransport = TransportWrapper.unwrap(wrappedForTransport, keyPair.getPrivate());

        assertTrue(Arrays.equals(dekViaKek, dekViaTransport));
        assertEquals(DekManager.DEK_LENGTH_BYTES, dekViaTransport.length);
    }

    @Test
    void issueWithoutRegisteredPublicKeyRejected() {
        String appId = "bulk-test-app-no-key";
        appRegistrationRepository.save(new AppRegistration(appId, "dek_issue,dek_unwrap", "no key", true));

        DekIssueRequest request = new DekIssueRequest(List.of(new DekIssueItem("row-1", null, null)));
        assertThrows(ApiException.class, () -> dekIssueService.issue(request, appId, "test-sub", "127.0.0.1"));
    }

    @Test
    void duplicateKeyInBatchRejectsWholeRequest() throws Exception {
        KeyPair keyPair = generateTestKeyPair();
        String appId = "bulk-test-app-dup";
        registerAppWithKeyPair(appId, keyPair.getPublic());

        DekIssueRequest request = new DekIssueRequest(List.of(
                new DekIssueItem("same-key", null, null),
                new DekIssueItem("same-key", null, null)
        ));
        assertThrows(ApiException.class, () -> dekIssueService.issue(request, appId, "test-sub", "127.0.0.1"));
    }

    @Test
    void namedDekReusedAcrossItemsSharesEdekIdAndMintsOnlyOnce() throws Exception {
        KeyPair keyPair = generateTestKeyPair();
        String appId = "bulk-test-app-named";
        registerAppWithKeyPair(appId, keyPair.getPublic());

        DekIssueRequest first = new DekIssueRequest(List.of(new DekIssueItem("row-1", "pii", "customers.ssn")));
        DekIssueResultItem firstItem = dekIssueService.issue(first, appId, "test-sub", "127.0.0.1").items().get(0);
        assertFalse(firstItem.reused());

        DekIssueRequest second = new DekIssueRequest(List.of(new DekIssueItem("row-2", "pii", "customers.ssn")));
        DekIssueResultItem secondItem = dekIssueService.issue(second, appId, "test-sub", "127.0.0.1").items().get(0);
        assertTrue(secondItem.reused());
        assertEquals(firstItem.edekId(), secondItem.edekId());

        byte[] dekViaFirstTransport = TransportWrapper.unwrap(
                Base64.getDecoder().decode(firstItem.wrappedDekB64()), keyPair.getPrivate());
        byte[] dekViaSecondTransport = TransportWrapper.unwrap(
                Base64.getDecoder().decode(secondItem.wrappedDekB64()), keyPair.getPrivate());
        assertTrue(Arrays.equals(dekViaFirstTransport, dekViaSecondTransport));

        assertEquals(1, edekRecordRepository.findAll().stream()
                .filter(r -> "customers.ssn".equals(r.getDekName())).count());
    }

    /**
     * Regression test for a real, confirmed bug: DekIssueResultItem used to omit
     * ownerAppId entirely, leaving a grant-authorized cross-app caller with no way
     * to know which app_id to use as the AES-GCM AAD for its own local encrypt.
     * Using the caller's own app_id (the only thing it had) produced a ciphertext
     * that nothing could ever decrypt again -- reproduced end-to-end against a
     * live server before this fix (see EncryptionService.ResolvedDek's javadoc for
     * the full reasoning; this test proves the same fix on the /dek/issue path).
     */
    @Test
    void crossAppEncryptGrantReuseReturnsCorrectOwnerAndProducesDecryptableCiphertext() throws Exception {
        KeyPair ownerKeys = generateTestKeyPair();
        KeyPair granteeKeys = generateTestKeyPair();
        String ownerAppId = "dek-issue-owner-1";
        String granteeAppId = "dek-issue-grantee-1";
        registerAppWithKeyPair(ownerAppId, ownerKeys.getPublic());
        registerAppWithKeyPair(granteeAppId, granteeKeys.getPublic());

        DekIssueRequest mintReq = new DekIssueRequest(List.of(new DekIssueItem("row-1", "pii", "cross.app.issue.column")));
        DekIssueResultItem minted = dekIssueService.issue(mintReq, ownerAppId, "test-sub", "127.0.0.1").items().get(0);
        assertEquals(ownerAppId, minted.ownerAppId());

        appGrantRepository.save(new AppGrant(granteeAppId, ownerAppId, "encrypt"));

        DekIssueRequest reuseReq = new DekIssueRequest(List.of(new DekIssueItem("row-1", "pii", "cross.app.issue.column")));
        DekIssueResultItem reused = dekIssueService.issue(reuseReq, granteeAppId, "test-sub", "127.0.0.1").items().get(0);
        assertTrue(reused.reused());
        assertEquals(minted.edekId(), reused.edekId());
        // The actual bug: ownership must NOT appear to transfer to the grantee.
        assertEquals(ownerAppId, reused.ownerAppId());

        // Grantee unwraps the transport-wrapped DEK with its own private key --
        // exactly what a real CLNT does -- then locally encrypts using
        // reused.ownerAppId() as AAD, per the contract this field now exists for.
        byte[] dek = TransportWrapper.unwrap(Base64.getDecoder().decode(reused.wrappedDekB64()), granteeKeys.getPrivate());
        byte[] plaintext = "grantee-produced ciphertext must stay decryptable".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        DekManager.EncryptResult encrypted = DekManager.encrypt(plaintext, dek, reused.ownerAppId());

        // Decrypt exactly as DecryptionService does: unwrap the same persisted EDEK
        // via the real KEK, and verify the AAD-bound tag with the record's true owner.
        EdekRecord record = edekRecordRepository.findById(reused.edekId()).orElseThrow();
        byte[] dekViaKek = kekClient.unwrapDek(Base64.getDecoder().decode(record.getEdekBlob()), record.getKekName(), record.getKekVersion());
        byte[] decrypted = DekManager.decrypt(encrypted.ciphertext(), encrypted.tag(), encrypted.iv(), dekViaKek, record.getAppId());
        assertEquals(new String(plaintext, java.nio.charset.StandardCharsets.UTF_8), new String(decrypted, java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    void namedDekConflictingClassificationRejected() throws Exception {
        KeyPair keyPair = generateTestKeyPair();
        String appId = "bulk-test-app-classification";
        registerAppWithKeyPair(appId, keyPair.getPublic());

        DekIssueRequest first = new DekIssueRequest(List.of(new DekIssueItem("row-1", "pii", "customers.classification-test-ssn")));
        dekIssueService.issue(first, appId, "test-sub", "127.0.0.1");

        DekIssueRequest conflicting = new DekIssueRequest(List.of(new DekIssueItem("row-2", "pci", "customers.classification-test-ssn")));
        DekIssueResultItem result = dekIssueService.issue(conflicting, appId, "test-sub", "127.0.0.1").items().get(0);
        assertEquals("error", result.status());
        assertTrue(result.detail().contains("already bound to data_classification"));
    }
}
