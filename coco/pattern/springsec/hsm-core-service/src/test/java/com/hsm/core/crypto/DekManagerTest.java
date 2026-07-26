package com.hsm.core.crypto;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.crypto.AEADBadTagException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DekManagerTest {

    @BeforeAll
    static void registerProvider() {
        TestFipsSupport.ensureReady();
    }

    @Test
    void generateDekProducesThirtyTwoBytes() {
        byte[] dek = DekManager.generateDek();
        assertEquals(32, dek.length);
    }

    @Test
    void encryptDecryptRoundTrip() throws AEADBadTagException {
        byte[] dek = DekManager.generateDek();
        byte[] plaintext = "top secret account number".getBytes(StandardCharsets.UTF_8);
        String appId = "payments-svc";

        DekManager.EncryptResult result = DekManager.encrypt(plaintext, dek, appId);
        assertEquals(DekManager.IV_LENGTH, result.iv().length);
        assertEquals(DekManager.TAG_LENGTH, result.tag().length);

        byte[] decrypted = DekManager.decrypt(result.ciphertext(), result.tag(), result.iv(), dek, appId);
        assertArrayEquals(plaintext, decrypted);
    }

    @Test
    void decryptFailsWithWrongAppIdAad() {
        byte[] dek = DekManager.generateDek();
        byte[] plaintext = "secret".getBytes(StandardCharsets.UTF_8);
        DekManager.EncryptResult result = DekManager.encrypt(plaintext, dek, "payments-svc");

        assertThrows(AEADBadTagException.class, () ->
                DekManager.decrypt(result.ciphertext(), result.tag(), result.iv(), dek, "reporting-app"));
    }

    @Test
    void decryptFailsOnTamperedCiphertext() {
        byte[] dek = DekManager.generateDek();
        byte[] plaintext = "secret".getBytes(StandardCharsets.UTF_8);
        String appId = "payments-svc";
        DekManager.EncryptResult result = DekManager.encrypt(plaintext, dek, appId);

        byte[] tampered = result.ciphertext().clone();
        tampered[0] ^= 0x01;

        assertThrows(AEADBadTagException.class, () ->
                DekManager.decrypt(tampered, result.tag(), result.iv(), dek, appId));
    }

    @Test
    void zeroDekOverwritesBytes() {
        byte[] dek = DekManager.generateDek();
        DekManager.zeroDek(dek);
        for (byte b : dek) {
            assertEquals(0, b);
        }
    }

    @Test
    void packAndUnpackTokenRoundTrip() {
        UUID edekId = UUID.randomUUID();
        byte[] iv = IvFactory.generate();
        byte[] tag = new byte[DekManager.TAG_LENGTH];
        byte[] ciphertext = "ciphertext-bytes-here".getBytes(StandardCharsets.UTF_8);

        String token = DekManager.packToken(edekId, iv, tag, ciphertext);
        assertTrue(token.startsWith("v1."));

        DekManager.UnpackedToken unpacked = DekManager.unpackToken(token);
        assertEquals(edekId, unpacked.edekId());
        assertArrayEquals(iv, unpacked.iv());
        assertArrayEquals(tag, unpacked.tag());
        assertArrayEquals(ciphertext, unpacked.ciphertext());
    }

    @Test
    void unpackTokenRejectsBadPrefix() {
        assertThrows(IllegalArgumentException.class, () -> DekManager.unpackToken("v2.abcdef"));
    }

    @Test
    void unpackTokenRejectsTooShortPayload() {
        String bogus = "v1." + java.util.Base64.getUrlEncoder().encodeToString(new byte[10]);
        assertThrows(IllegalArgumentException.class, () -> DekManager.unpackToken(bogus));
    }

    @Test
    void makeFingerprintIsDeterministicAndDetectsMismatch() {
        byte[] iv = IvFactory.generate();
        byte[] tag = new byte[DekManager.TAG_LENGTH];
        String fp1 = DekManager.makeFingerprint(iv, tag);
        String fp2 = DekManager.makeFingerprint(iv, tag);
        assertEquals(fp1, fp2);
        assertEquals(16, fp1.length());

        byte[] otherIv = IvFactory.generate();
        String fp3 = DekManager.makeFingerprint(otherIv, tag);
        assertNotEquals(fp1, fp3);
    }
}
