package com.hsm.bulk.service;

import com.hsm.bulk.crypto.DekManager;
import com.hsm.bulk.crypto.KekClient;
import com.hsm.bulk.crypto.TransportWrapper;
import com.hsm.bulk.dto.DekUnwrapItem;
import com.hsm.bulk.dto.DekUnwrapRequest;
import com.hsm.bulk.dto.DekUnwrapResponse;
import com.hsm.bulk.dto.DekUnwrapResultItem;
import com.hsm.bulk.model.AppDecryptGrant;
import com.hsm.bulk.model.AppRegistration;
import com.hsm.bulk.model.EdekRecord;
import com.hsm.bulk.repository.AppDecryptGrantRepository;
import com.hsm.bulk.repository.AppRegistrationRepository;
import com.hsm.bulk.repository.EdekRecordRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class DekUnwrapServiceTest {

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:dekunwrap-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private DekUnwrapService dekUnwrapService;

    @Autowired
    private AppRegistrationRepository appRegistrationRepository;

    @Autowired
    private AppDecryptGrantRepository grantRepository;

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

    private void registerApp(String appId, java.security.PublicKey publicKey) {
        appRegistrationRepository.save(new AppRegistration(
                appId, "dek_issue,dek_unwrap", "test app", true, pem(publicKey.getEncoded(), "PUBLIC KEY")));
    }

    /** Seeds an EdekRecord exactly as DekIssueService (or EncryptionService) would, without going through the service under test. */
    private UUID seedEdekRecordOwnedBy(String ownerAppId, byte[] rawDek) {
        KekClient.WrapResult wrapResult = kekClient.wrapDek(rawDek);
        UUID edekId = UUID.randomUUID();
        edekRecordRepository.save(new EdekRecord(
                edekId, ownerAppId, Base64.getEncoder().encodeToString(wrapResult.edekBytes()), wrapResult.kekVersion(),
                DekManager.ALGORITHM, "utf8", null, null, null));
        return edekId;
    }

    @Test
    void ownerAppUnwrapsItsOwnRecordAndTransportUnwrapMatchesOriginalDek() throws Exception {
        KeyPair keyPair = generateTestKeyPair();
        String ownerAppId = "unwrap-owner-1";
        registerApp(ownerAppId, keyPair.getPublic());

        byte[] rawDek = DekManager.generateDek();
        UUID edekId = seedEdekRecordOwnedBy(ownerAppId, rawDek);

        DekUnwrapRequest request = new DekUnwrapRequest(List.of(new DekUnwrapItem("row-1", edekId)));
        DekUnwrapResponse response = dekUnwrapService.unwrap(request, ownerAppId, "sub", List.of("dek_unwrap"), "127.0.0.1");

        assertEquals(1, response.items().size());
        DekUnwrapResultItem item = response.items().get(0);
        assertEquals("success", item.status());

        byte[] wrappedForTransport = Base64.getDecoder().decode(item.wrappedDekB64());
        byte[] unwrapped = TransportWrapper.unwrap(wrappedForTransport, keyPair.getPrivate());
        assertTrue(Arrays.equals(rawDek, unwrapped));
    }

    @Test
    void nonOwnerWithoutGrantDeniedPerItem() throws Exception {
        KeyPair ownerKeys = generateTestKeyPair();
        KeyPair granteeKeys = generateTestKeyPair();
        String ownerAppId = "unwrap-owner-2";
        String granteeAppId = "unwrap-grantee-2";
        registerApp(ownerAppId, ownerKeys.getPublic());
        registerApp(granteeAppId, granteeKeys.getPublic());

        UUID edekId = seedEdekRecordOwnedBy(ownerAppId, DekManager.generateDek());

        DekUnwrapRequest request = new DekUnwrapRequest(List.of(new DekUnwrapItem("row-1", edekId)));
        DekUnwrapResponse response = dekUnwrapService.unwrap(request, granteeAppId, "sub", List.of("dek_unwrap"), "127.0.0.1");

        DekUnwrapResultItem item = response.items().get(0);
        assertEquals("error", item.status());
        assertTrue(item.detail().toLowerCase().contains("access denied"));
    }

    @Test
    void nonOwnerWithExplicitGrantSucceeds() throws Exception {
        KeyPair ownerKeys = generateTestKeyPair();
        KeyPair granteeKeys = generateTestKeyPair();
        String ownerAppId = "unwrap-owner-3";
        String granteeAppId = "unwrap-grantee-3";
        registerApp(ownerAppId, ownerKeys.getPublic());
        registerApp(granteeAppId, granteeKeys.getPublic());
        grantRepository.save(new AppDecryptGrant(granteeAppId, ownerAppId));

        byte[] rawDek = DekManager.generateDek();
        UUID edekId = seedEdekRecordOwnedBy(ownerAppId, rawDek);

        DekUnwrapRequest request = new DekUnwrapRequest(List.of(new DekUnwrapItem("row-1", edekId)));
        DekUnwrapResponse response = dekUnwrapService.unwrap(request, granteeAppId, "sub", List.of("dek_unwrap"), "127.0.0.1");

        DekUnwrapResultItem item = response.items().get(0);
        assertEquals("success", item.status());
        byte[] unwrapped = TransportWrapper.unwrap(Base64.getDecoder().decode(item.wrappedDekB64()), granteeKeys.getPrivate());
        assertTrue(Arrays.equals(rawDek, unwrapped));
    }

    @Test
    void governanceScopeBypassesGrantCheck() throws Exception {
        KeyPair ownerKeys = generateTestKeyPair();
        KeyPair governanceKeys = generateTestKeyPair();
        String ownerAppId = "unwrap-owner-4";
        String governanceAppId = "unwrap-governance-4";
        registerApp(ownerAppId, ownerKeys.getPublic());
        registerApp(governanceAppId, governanceKeys.getPublic());

        UUID edekId = seedEdekRecordOwnedBy(ownerAppId, DekManager.generateDek());

        DekUnwrapRequest request = new DekUnwrapRequest(List.of(new DekUnwrapItem("row-1", edekId)));
        DekUnwrapResponse response = dekUnwrapService.unwrap(
                request, governanceAppId, "sub", List.of("dek_unwrap", "governance"), "127.0.0.1");

        assertEquals("success", response.items().get(0).status());
    }
}
