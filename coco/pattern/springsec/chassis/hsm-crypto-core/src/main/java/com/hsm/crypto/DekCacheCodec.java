package com.hsm.crypto;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * Key and value format of the Redis DEK cache, shared by hsm-core-service (reads and
 * writes entries) and hsm-cache-key-rotator (rekeys or flushes them on a CEK rotation)
 * -- java/docs/COMPATIBILITY_CONTRACT.md C7, pinned by
 * java/chassis/test-vectors/crypto-v1.json.
 *
 * <pre>
 *   version = slot ":" kv_version          e.g. "alpha:0f4a6c2e..."
 *   key     = "dek:" version ":" edek_id   UTF-8
 *   value   = base64(iv) ":" base64(ciphertext || tag)
 *             standard base64, ASCII; AES-256-GCM under the CEK, 12-byte IV, no AAD
 * </pre>
 *
 * <p>The DEK is never stored in Redis in the clear: a reader needs the CEK, which
 * lives only in Key Vault (cek-alpha / cek-beta) and in service memory.
 */
public final class DekCacheCodec {

    public static final String KEY_PREFIX = "dek:";
    static final int NONCE_LENGTH = 12;

    private static final String CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String PROVIDER = BouncyCastleFipsProvider.PROVIDER_NAME;
    private static final int TAG_LENGTH_BITS = 128;

    static {
        FipsBootstrap.register(); // before RANDOM is captured -- see FipsBootstrap
    }

    private static final SecureRandom RANDOM = CryptoServicesRegistrar.getSecureRandom();

    private DekCacheCodec() {
    }

    /** The CEK version component of a cache key: the active Key Vault slot plus that secret's version. */
    public static String version(String slot, String kvVersion) {
        return slot + ":" + kvVersion;
    }

    public static byte[] key(String version, String edekId) {
        return (KEY_PREFIX + version + ":" + edekId).getBytes(StandardCharsets.UTF_8);
    }

    /** Encrypts a DEK for storage under the given CEK, with a fresh random IV. */
    public static byte[] encode(byte[] dek, byte[] cek) {
        byte[] iv = new byte[NONCE_LENGTH];
        RANDOM.nextBytes(iv);
        return encode(dek, cek, iv);
    }

    /** Fixed-IV form, for the test vectors only -- an IV must never be reused under one CEK. */
    static byte[] encode(byte[] dek, byte[] cek, byte[] iv) {
        byte[] ciphertext = gcm(Cipher.ENCRYPT_MODE, cek, iv, dek);
        String encoded = Base64.getEncoder().encodeToString(iv) + ":" + Base64.getEncoder().encodeToString(ciphertext);
        return encoded.getBytes(StandardCharsets.US_ASCII);
    }

    /**
     * Decrypts a cached value. Throws {@link IllegalArgumentException} if the value is
     * not in the expected format, {@link IllegalStateException} if it does not
     * authenticate under this CEK.
     */
    public static byte[] decode(byte[] value, byte[] cek) {
        String s = new String(value, StandardCharsets.US_ASCII);
        int idx = s.indexOf(':');
        if (idx <= 0 || idx == s.length() - 1) {
            throw new IllegalArgumentException("DEK cache value is not in iv_b64:ciphertext_b64 form");
        }
        byte[] iv = Base64.getDecoder().decode(s.substring(0, idx));
        byte[] ciphertext = Base64.getDecoder().decode(s.substring(idx + 1));
        return gcm(Cipher.DECRYPT_MODE, cek, iv, ciphertext);
    }

    private static byte[] gcm(int mode, byte[] key, byte[] iv, byte[] input) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION, PROVIDER);
            cipher.init(mode, new SecretKeySpec(key, "AES"), new GCMParameterSpec(TAG_LENGTH_BITS, iv));
            return cipher.doFinal(input);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("DEK cache AES-GCM operation failed", e);
        }
    }
}
