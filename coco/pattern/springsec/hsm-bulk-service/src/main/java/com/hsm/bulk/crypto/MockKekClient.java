package com.hsm.bulk.crypto;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-memory stand-in for Azure Key Vault, letting the BulkVsBatchBenchmark PoC run
 * fully locally without a real Key Vault -- mirrors hsm-core-service's own
 * SKIP_AKV/demo-mode MockKekClient pattern (com.hsm.core.crypto.MockKekClient),
 * trimmed to just what /dek/issue and /dek/unwrap need (no rotateToNewVersion/
 * getState demo-UI introspection, since this module has no such UI).
 *
 * <p>Deliberately uses the SAME fixed key bytes and version string
 * ("demo-v1") as hsm-core-service's own MockKekClient's DEMO_V1_KEY/
 * INITIAL_VERSION, rather than independent fake material -- there's no real
 * Key Vault for two separate mock instances to actually share, so this is
 * the only way an EDEK issued here can be unwrapped by hsm-core-service's
 * real /decrypt in the benchmark's token-format-compatibility check. Both
 * are hardcoded/deterministic and neither process rotates mid-benchmark, so
 * this holds for the PoC; it would NOT hold against a real Key Vault, where
 * hsm-bulk-service's real AzureKeyVaultKekClient (mockKek=false) is used
 * instead and both services genuinely share the same HSM-backed key.
 */
public class MockKekClient implements KekClient {

    private static final String CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String PROVIDER = BouncyCastleFipsProvider.PROVIDER_NAME;
    private static final int TAG_LENGTH_BITS = 128;
    private static final int NONCE_LENGTH = 12;
    private static final String CURRENT_VERSION = "demo-v1";

    private static final byte[] KEK_BYTES = {
            (byte) 0xd1, (byte) 0xe5, (byte) 0xa2, (byte) 0xc9, (byte) 0xf4, (byte) 0x7b, (byte) 0x38, (byte) 0x06,
            (byte) 0xe9, (byte) 0xc2, (byte) 0xf5, (byte) 0xa8, (byte) 0xd1, (byte) 0xb4, (byte) 0xe7, (byte) 0x0c,
            (byte) 0x3f, (byte) 0x6a, (byte) 0x9d, (byte) 0x2c, (byte) 0x5b, (byte) 0x8e, (byte) 0x1f, (byte) 0x47,
            (byte) 0xa0, (byte) 0xd3, (byte) 0xc6, (byte) 0xb9, (byte) 0xe2, (byte) 0xf5, (byte) 0xa8, (byte) 0xd1
    };

    private final SecureRandom random = CryptoServicesRegistrar.getSecureRandom();
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, String> secrets = new LinkedHashMap<>();

    /** Seed a fake secret value for local testing (e.g. cek-current-key equivalents are not used here, but kept for parity). */
    public void putSecret(String name, String value) {
        secrets.put(name, value);
    }

    @Override
    public WrapResult wrapDek(byte[] dek) {
        lock.lock();
        try {
            byte[] nonce = new byte[NONCE_LENGTH];
            random.nextBytes(nonce);
            byte[] wrapped = gcmEncrypt(KEK_BYTES, nonce, dek);
            byte[] edek = new byte[nonce.length + wrapped.length];
            System.arraycopy(nonce, 0, edek, 0, nonce.length);
            System.arraycopy(wrapped, 0, edek, nonce.length, wrapped.length);
            return new WrapResult(edek, CURRENT_VERSION);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] unwrapDek(byte[] edek, String kekVersion) {
        if (!CURRENT_VERSION.equals(kekVersion)) {
            throw new IllegalArgumentException("Unknown mock KEK version: " + kekVersion);
        }
        byte[] nonce = Arrays.copyOfRange(edek, 0, NONCE_LENGTH);
        byte[] wrapped = Arrays.copyOfRange(edek, NONCE_LENGTH, edek.length);
        return gcmDecrypt(KEK_BYTES, nonce, wrapped);
    }

    @Override
    public String getCurrentKekVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public String fetchSecret(String secretName) {
        return secrets.getOrDefault(secretName, "");
    }

    @Override
    public SecretWithVersion fetchSecretWithVersion(String secretName) {
        return new SecretWithVersion(fetchSecret(secretName), "");
    }

    @Override
    public void close() {
        // no-op
    }

    private static byte[] gcmEncrypt(byte[] key, byte[] nonce, byte[] plaintext) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION, PROVIDER);
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(TAG_LENGTH_BITS, nonce));
            return cipher.doFinal(plaintext);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Mock KEK wrap failed", e);
        }
    }

    private static byte[] gcmDecrypt(byte[] key, byte[] nonce, byte[] ciphertext) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION, PROVIDER);
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(TAG_LENGTH_BITS, nonce));
            return cipher.doFinal(ciphertext);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Mock KEK unwrap failed", e);
        }
    }
}
