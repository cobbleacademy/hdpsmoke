package com.hsm.cekrotation;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Smoke test for the AES-256-GCM blob encrypt/decrypt round trip used by
 * RedisOps.rekeyDekCache -- verified indirectly via reflection since the blob
 * helpers are private and the class otherwise requires a live Redis connection.
 */
class RedisOpsTest {

    @BeforeAll
    static void registerProvider() {
        TestFipsSupport.ensureReady();
    }

    @Test
    void encryptDecryptBlobRoundTrip() throws Exception {
        RedisOps ops = new RedisOps(null);
        byte[] cek = new byte[32];
        byte[] iv = new byte[12];
        byte[] plaintext = "unit-test-dek-bytes-1234567890ab".getBytes();

        Method encrypt = RedisOps.class.getDeclaredMethod("encryptBlob", byte[].class, byte[].class, byte[].class);
        encrypt.setAccessible(true);
        Method decrypt = RedisOps.class.getDeclaredMethod("decryptBlob", byte[].class, byte[].class);
        decrypt.setAccessible(true);

        assertDoesNotThrow(() -> {
            byte[] blob = (byte[]) encrypt.invoke(ops, plaintext, cek, iv);
            byte[] roundTripped = (byte[]) decrypt.invoke(ops, blob, cek);
            if (!java.util.Arrays.equals(plaintext, roundTripped)) {
                throw new AssertionError("round trip mismatch");
            }
        });
    }
}
