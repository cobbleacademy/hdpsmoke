package com.hsm.client.deid;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FieldDecryptorTest {

    private final DeidTestSupport.FakeHsm hsm = new DeidTestSupport.FakeHsm();
    private final FieldDecryptor decryptor = hsm.decryptor();

    @Test
    void decryptsHsmCiphertextAndRecordsTheKey() {
        String token = hsm.encrypt("6035712345678901");
        assertTrue(token.startsWith("v1."));
        assertEquals("6035712345678901", decryptor.decrypt(token));
        assertEquals("6035712345678901", decryptor.decrypt(" " + token + " "), "surrounding spaces ignored");
        assertEquals(Set.of(hsm.columnDek), decryptor.edekIdsSeen());
    }

    @Test
    void nullAndEmptyPassThrough() {
        assertNull(decryptor.decrypt(null));
        assertEquals("", decryptor.decrypt(""));
    }

    @Test
    void plainTextInAnEncryptedColumnFailsWithoutTheValue() {
        DeidException e = assertThrows(DeidException.class, () -> decryptor.decrypt("6035712345678901"));
        assertTrue(e.getMessage().contains("not an HSM ciphertext"));
        assertFalse(e.getMessage().contains("6035712345678901"));
    }

    @Test
    void tamperedCiphertextFails() {
        com.hsm.crypto.DekManager.UnpackedToken t = com.hsm.crypto.DekManager.unpackToken(hsm.encrypt("jane.doe@mail.com"));
        byte[] ct = t.ciphertext().clone();
        ct[0] ^= 1;
        String tampered = com.hsm.crypto.DekManager.packToken(t.edekId(), t.iv(), t.tag(), ct);
        DeidException e = assertThrows(DeidException.class, () -> decryptor.decrypt(tampered));
        assertTrue(e.getMessage().contains("decrypt failed"));
        assertFalse(e.getMessage().contains(tampered));
    }

    @Test
    void aKeyWithoutAGrantFailsNamingTheKey() {
        String token = hsm.encrypt("M1234567");
        hsm.deks.clear();
        DeidException e = assertThrows(DeidException.class, () -> decryptor.decrypt(token));
        assertTrue(e.getMessage().contains(hsm.columnDek.toString()));
        assertTrue(e.getMessage().contains("grant"));
    }

    @Test
    void looksEncryptedSpotsCiphertext() {
        assertTrue(FieldDecryptor.looksEncrypted(hsm.encrypt("x")));
        assertFalse(FieldDecryptor.looksEncrypted("v1.not-really"));
        assertFalse(FieldDecryptor.looksEncrypted("6035712345678901"));
        assertFalse(FieldDecryptor.looksEncrypted(null));
    }
}
