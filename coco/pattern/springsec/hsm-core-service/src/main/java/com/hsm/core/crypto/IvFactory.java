package com.hsm.core.crypto;

import org.bouncycastle.crypto.CryptoServicesRegistrar;

import java.security.SecureRandom;

/**
 * IV generation — 96-bit (12 bytes) random IV per NIST SP 800-38D §8.2.
 * Each call to generate() produces a fresh cryptographically secure value,
 * drawn from BC-FIPS's approved-mode DRBG-backed SecureRandom.
 */
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
