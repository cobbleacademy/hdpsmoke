package com.hsm.core.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** How RotationService names its per-crypto-schema locks. */
class RotationLockKeyTest {

    @Test
    void lockNameIsPrefixPlusCryptoSchema() {
        assertEquals("hsm:kek-wrap:hsm_crypto_dev", RotationService.lockKey(RotationService.KEK_WRAP_LOCK, "hsm_crypto_dev"));
        assertEquals("hsm:named-dek-rotation:public", RotationService.lockKey(RotationService.NAMED_DEK_ROTATION_LOCK, "public"));
    }

    @Test
    void sameSchemaSpelledDifferentlyGetsTheSameLock() {
        // Postgres folds unquoted names to lower case, so these are one schema.
        assertEquals(RotationService.lockKey(RotationService.KEK_WRAP_LOCK, "hsm_crypto_dev"),
                RotationService.lockKey(RotationService.KEK_WRAP_LOCK, " HSM_Crypto_Dev "));
    }

    @Test
    void differentEnvironmentsGetDifferentLocks() {
        assertNotEquals(RotationService.lockKey(RotationService.KEK_WRAP_LOCK, "hsm_crypto_dev"),
                RotationService.lockKey(RotationService.KEK_WRAP_LOCK, "hsm_crypto_test"));
    }

    @Test
    void emptyCryptoSchemaIsRefused() {
        assertThrows(IllegalStateException.class, () -> RotationService.lockKey(RotationService.KEK_WRAP_LOCK, ""));
        assertThrows(IllegalStateException.class, () -> RotationService.lockKey(RotationService.KEK_WRAP_LOCK, "  "));
        assertThrows(IllegalStateException.class, () -> RotationService.lockKey(RotationService.KEK_WRAP_LOCK, null));
    }
}
