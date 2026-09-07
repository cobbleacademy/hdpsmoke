package com.hsm.client;

import com.hsm.client.config.FipsBootstrap;
import com.hsm.client.crypto.DekManager;
import com.hsm.client.crypto.TransportWrapper;
import com.hsm.client.svc.SvcClient;
import com.hsm.client.svc.SvcConfig;
import org.junit.jupiter.api.Test;

import javax.crypto.AEADBadTagException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Regression coverage for a real, confirmed bug: HsmCryptoClient used to
 * encrypt/decrypt using its own configured appId as the AES-GCM AAD,
 * regardless of who /dek/issue or /dek/unwrap actually reported as the
 * DEK's owner. That's correct only when the caller happens to be the
 * owner -- a grant-authorized cross-app dek_name reuse silently produced a
 * ciphertext nothing could ever decrypt again. See
 * hsm-core-service's EncryptionService.ResolvedDek javadoc for the full
 * story (this is the JVM-client twin of that same fix).
 *
 * <p>Uses a FakeSvcClient (a real SvcClient subclass overriding issue/unwrap,
 * no network) since SvcClient's own HTTP path has no injectable seam and this
 * module's test suite otherwise runs with no external dependencies at all.
 */
class HsmCryptoClientTest {

    static {
        FipsBootstrap.register();
    }

    private static KeyPair generateKeyPair() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        return gen.generateKeyPair();
    }

    private static SvcConfig dummyConfig(String appId) {
        return new SvcConfig(
                "http://unused.invalid", "/api/sensec/hsm/v1", appId, SvcConfig.AuthMode.STATIC,
                "unused-token", null, 100, "unused", null, null, null, null);
    }

    /** Returns canned results without ever making a network call. */
    private static class FakeSvcClient extends SvcClient {
        private final List<SvcClient.IssueResult> issueResults;
        private final List<SvcClient.UnwrapResult> unwrapResults;

        FakeSvcClient(SvcConfig config, List<SvcClient.IssueResult> issueResults, List<SvcClient.UnwrapResult> unwrapResults) {
            super(config);
            this.issueResults = issueResults;
            this.unwrapResults = unwrapResults;
        }

        @Override
        public List<SvcClient.IssueResult> issue(List<SvcClient.IssueItem> items) {
            return issueResults;
        }

        @Override
        public List<SvcClient.UnwrapResult> unwrap(List<SvcClient.UnwrapItem> items) {
            return unwrapResults;
        }
    }

    @Test
    void encryptUsesReturnedOwnerAppIdNotThisClientsOwnIdentity() throws Exception {
        KeyPair keyPair = generateKeyPair();
        String thisClientsOwnAppId = "this-client-app";
        String trueOwner = "the-actual-owner-app";   // deliberately different

        byte[] rawDek = DekManager.generateDek();
        byte[] wrapped = TransportWrapper.wrap(rawDek, keyPair.getPublic());
        UUID edekId = UUID.randomUUID();

        FakeSvcClient fake = new FakeSvcClient(dummyConfig(thisClientsOwnAppId),
                List.of(new SvcClient.IssueResult("encrypt", "success", edekId,
                        Base64.getEncoder().encodeToString(wrapped), trueOwner, null, false)),
                List.of());

        try (HsmCryptoClient client = new HsmCryptoClient(fake, keyPair.getPrivate(), thisClientsOwnAppId, 1000, Duration.ofMinutes(30))) {
            String token = client.encrypt("payload", "some.dek.name");

            DekManager.UnpackedToken unpacked = DekManager.unpackToken(token);
            // Must decrypt correctly against the TRUE owner...
            byte[] decrypted = DekManager.decrypt(unpacked.ciphertext(), unpacked.tag(), unpacked.iv(), rawDek, trueOwner);
            assertEquals("payload", new String(decrypted, StandardCharsets.UTF_8));

            // ...and must NOT decrypt against this client's own identity -- the exact bug this test guards against.
            assertThrows(AEADBadTagException.class, () ->
                    DekManager.decrypt(unpacked.ciphertext(), unpacked.tag(), unpacked.iv(), rawDek, thisClientsOwnAppId));
        }
    }

    @Test
    void decryptUsesReturnedOwnerAppIdNotThisClientsOwnIdentity() throws Exception {
        KeyPair keyPair = generateKeyPair();
        String thisClientsOwnAppId = "this-client-app";
        String trueOwner = "the-actual-owner-app";

        byte[] rawDek = DekManager.generateDek();
        byte[] wrapped = TransportWrapper.wrap(rawDek, keyPair.getPublic());
        UUID edekId = UUID.randomUUID();

        // Simulate a ciphertext genuinely produced by the true owner.
        DekManager.EncryptResult encrypted = DekManager.encrypt("owner wrote this".getBytes(StandardCharsets.UTF_8), rawDek, trueOwner);
        String token = DekManager.packToken(edekId, encrypted.iv(), encrypted.tag(), encrypted.ciphertext());

        FakeSvcClient fake = new FakeSvcClient(dummyConfig(thisClientsOwnAppId),
                List.of(),
                List.of(new SvcClient.UnwrapResult("decrypt", "success", edekId,
                        Base64.getEncoder().encodeToString(wrapped), trueOwner, null)));

        try (HsmCryptoClient client = new HsmCryptoClient(fake, keyPair.getPrivate(), thisClientsOwnAppId, 1000, Duration.ofMinutes(30))) {
            String decrypted = client.decryptToString(token);
            assertEquals("owner wrote this", decrypted);
        }
    }

    @Test
    void encryptCacheHitsSvcClientOnceForRepeatedDekName() throws Exception {
        KeyPair keyPair = generateKeyPair();
        byte[] rawDek = DekManager.generateDek();
        byte[] wrapped = TransportWrapper.wrap(rawDek, keyPair.getPublic());
        UUID edekId = UUID.randomUUID();

        java.util.concurrent.atomic.AtomicInteger issueCalls = new java.util.concurrent.atomic.AtomicInteger();
        SvcConfig config = dummyConfig("app-a");
        FakeSvcClient fake = new FakeSvcClient(config,
                List.of(new SvcClient.IssueResult("encrypt", "success", edekId,
                        Base64.getEncoder().encodeToString(wrapped), "app-a", null, false)),
                List.of()) {
            @Override
            public List<SvcClient.IssueResult> issue(List<SvcClient.IssueItem> items) {
                issueCalls.incrementAndGet();
                return super.issue(items);
            }
        };

        try (HsmCryptoClient client = new HsmCryptoClient(fake, keyPair.getPrivate(), "app-a", 1000, Duration.ofMinutes(30))) {
            client.encrypt("one", "reused.name");
            client.encrypt("two", "reused.name");
            client.encrypt("three", "reused.name");
        }

        assertEquals(1, issueCalls.get());
    }
}
