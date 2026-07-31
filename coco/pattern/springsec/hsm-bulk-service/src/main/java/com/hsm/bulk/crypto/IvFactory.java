package com.hsm.bulk.crypto;

import org.bouncycastle.crypto.CryptoServicesRegistrar;

import java.security.SecureRandom;

/** Duplicated verbatim from com.hsm.core.crypto.IvFactory -- see DekManager's class comment. */
public final class IvFactory {

    public static final int IV_LENGTH_BYTES = 12; // 96-bit recommended for AES-GCM

    private static final SecureRandom RANDOM = CryptoServicesRegistrar.getSecureRandom();

    private IvFactory() {
    }

    public static byte[] generate() {
        byte[] iv = new byte[IV_LENGTH_BYTES];
        RANDOM.nextBytes(iv);
        return iv;
    }
}
