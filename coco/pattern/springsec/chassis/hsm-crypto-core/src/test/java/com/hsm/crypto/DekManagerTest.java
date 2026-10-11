package com.hsm.crypto;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.junit.jupiter.api.Test;

import javax.crypto.AEADBadTagException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Behaviour the fixed vectors can't cover: fresh randomness per call, and the library registering BC-FIPS itself. */
class DekManagerTest {

    @Test
    void usingTheLibraryLeavesBcFipsRegisteredInApprovedOnlyMode() {
        DekManager.generateDek();
        assertTrue(CryptoServicesRegistrar.isInApprovedOnlyMode());
    }

    @Test
    void encryptPackUnpackDecryptRoundTrip() throws AEADBadTagException {
        byte[] dek = DekManager.generateDek();
        byte[] plaintext = "round trip ✓".getBytes(StandardCharsets.UTF_8);
        UUID edekId = UUID.randomUUID();

        DekManager.EncryptResult enc = DekManager.encrypt(plaintext, dek, "payments-svc");
        DekManager.UnpackedToken token = DekManager.unpackToken(DekManager.packToken(edekId, enc.iv(), enc.tag(), enc.ciphertext()));

        assertEquals(edekId, token.edekId());
        assertArrayEquals(plaintext, DekManager.decrypt(token.ciphertext(), token.tag(), token.iv(), dek, "payments-svc"));
        assertThrows(AEADBadTagException.class,
                () -> DekManager.decrypt(token.ciphertext(), token.tag(), token.iv(), dek, "other-app"));
    }

    @Test
    void everyEncryptionUsesAFreshIvAndEveryDekIsFresh() {
        byte[] dek = DekManager.generateDek();
        Set<String> ivs = new HashSet<>();
        Set<String> deks = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            ivs.add(HexFormat.of().formatHex(DekManager.encrypt(new byte[]{1}, dek, "a").iv()));
            deks.add(HexFormat.of().formatHex(DekManager.generateDek()));
        }
        assertEquals(1000, ivs.size());
        assertEquals(1000, deks.size());
    }

    @Test
    void dekCacheCodecRoundTripsWithRandomIv() {
        byte[] cek = DekManager.generateDek();
        byte[] dek = DekManager.generateDek();
        assertArrayEquals(dek, DekCacheCodec.decode(DekCacheCodec.encode(dek, cek), cek));
        assertThrows(IllegalStateException.class, () -> DekCacheCodec.decode(DekCacheCodec.encode(dek, cek), DekManager.generateDek()));
        assertThrows(IllegalArgumentException.class, () -> DekCacheCodec.decode("no-separator".getBytes(StandardCharsets.US_ASCII), cek));
    }
}
