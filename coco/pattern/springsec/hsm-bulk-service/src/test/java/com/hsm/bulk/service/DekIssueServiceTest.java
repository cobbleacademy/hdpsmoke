package com.hsm.bulk.service;

import com.hsm.bulk.crypto.DekManager;
import com.hsm.bulk.crypto.KekClient;
import com.hsm.bulk.crypto.TransportWrapper;
import com.hsm.bulk.dto.DekIssueItem;
import com.hsm.bulk.dto.DekIssueRequest;
import com.hsm.bulk.dto.DekIssueResponse;
import com.hsm.bulk.dto.DekIssueResultItem;
import com.hsm.bulk.model.AppRegistration;
import com.hsm.bulk.model.EdekRecord;
import com.hsm.bulk.repository.AppRegistrationRepository;
import com.hsm.bulk.repository.EdekRecordRepository;
import com.hsm.bulk.web.ApiException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
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
        appRegistrationRepository.save(new AppRegistration(appId, "dek_issue,dek_unwrap", "test app", true, pem));
        return pem;
    }

    @Test
    void issuedDekPersistsAsPlainEdekRecordAndTransportUnwrapsToKekWrappedValue() throws Exception {
        KeyPair keyPair = generateTestKeyPair();
        String appId = "bulk-test-app-1";
        registerAppWithKeyPair(appId, keyPair.getPublic());

        DekIssueRequest request = new DekIssueRequest(List.of(new DekIssueItem("row-1", "pii")));
        DekIssueResponse response = dekIssueService.issue(request, appId, "test-sub", "127.0.0.1");

        assertEquals(1, response.items().size());
        DekIssueResultItem item = response.items().get(0);
        assertEquals("success", item.status());
        assertEquals("row-1", item.key());
        assertNotNull(item.edekId());
        assertNotNull(item.wrappedDekB64());

        // Persisted shape matches what EncryptionService.encrypt would write, minus the AES-GCM fields.
        Optional<EdekRecord> maybeRecord = edekRecordRepository.findById(item.edekId());
        assertTrue(maybeRecord.isPresent());
        EdekRecord record = maybeRecord.get();
        assertEquals(appId, record.getAppId());
        assertEquals(DekManager.ALGORITHM, record.getAlgorithm());
        assertEquals("utf8", record.getEncoding());
        assertEquals("pii", record.getDataClassification());
        assertEquals(com.hsm.bulk.model.RotationStatus.CURRENT, record.getRotationStatus());
        assertNull(record.getFingerprint());

        // Round-trip: unwrap the KEK-wrapped EDEK (as /decrypt would) and separately RSA-unwrap the
        // transport wrap (as CLNT would) -- both must yield the exact same raw DEK bytes.
        byte[] edekBytes = Base64.getDecoder().decode(record.getEdekBlob());
        byte[] dekViaKek = kekClient.unwrapDek(edekBytes, record.getKekVersion());

        byte[] wrappedForTransport = Base64.getDecoder().decode(item.wrappedDekB64());
        byte[] dekViaTransport = TransportWrapper.unwrap(wrappedForTransport, keyPair.getPrivate());

        assertTrue(Arrays.equals(dekViaKek, dekViaTransport));
        assertEquals(DekManager.DEK_LENGTH_BYTES, dekViaTransport.length);
    }

    @Test
    void issueWithoutRegisteredPublicKeyRejected() {
        String appId = "bulk-test-app-no-key";
        appRegistrationRepository.save(new AppRegistration(appId, "dek_issue,dek_unwrap", "no key", true, null));

        DekIssueRequest request = new DekIssueRequest(List.of(new DekIssueItem("row-1", null)));
        assertThrows(ApiException.class, () -> dekIssueService.issue(request, appId, "test-sub", "127.0.0.1"));
    }

    @Test
    void duplicateKeyInBatchRejectsWholeRequest() throws Exception {
        KeyPair keyPair = generateTestKeyPair();
        String appId = "bulk-test-app-dup";
        registerAppWithKeyPair(appId, keyPair.getPublic());

        DekIssueRequest request = new DekIssueRequest(List.of(
                new DekIssueItem("same-key", null),
                new DekIssueItem("same-key", null)
        ));
        assertThrows(ApiException.class, () -> dekIssueService.issue(request, appId, "test-sub", "127.0.0.1"));
    }
}
