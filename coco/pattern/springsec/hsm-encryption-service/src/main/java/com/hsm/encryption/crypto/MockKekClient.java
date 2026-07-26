package com.hsm.encryption.crypto;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * In-memory stand-in for Azure Key Vault, used only under DEMO_MODE=true.
 * Ported from app/demo/mock_kek_client.py -- same interface as the real
 * client so it's a drop-in swap; no other code needs to change in demo mode.
 */
public class MockKekClient implements KekClient {

    private static final String CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String PROVIDER = BouncyCastleFipsProvider.PROVIDER_NAME;
    private static final int TAG_LENGTH_BITS = 128;
    private static final int NONCE_LENGTH = 12;
    private static final String INITIAL_VERSION = "demo-v1";

    // Fixed (not regenerated on every boot) so persisted demo EDEKs remain decryptable
    // across restarts of the demo DB -- mirrors Python's hardcoded _DEMO_V1_KEY, though
    // not byte-for-byte identical: that literal decodes to a non-standard AES key length.
    private static final byte[] DEMO_V1_KEY = {
            (byte) 0xd1, (byte) 0xe5, (byte) 0xa2, (byte) 0xc9, (byte) 0xf4, (byte) 0x7b, (byte) 0x38, (byte) 0x06,
            (byte) 0xe9, (byte) 0xc2, (byte) 0xf5, (byte) 0xa8, (byte) 0xd1, (byte) 0xb4, (byte) 0xe7, (byte) 0x0c,
            (byte) 0x3f, (byte) 0x6a, (byte) 0x9d, (byte) 0x2c, (byte) 0x5b, (byte) 0x8e, (byte) 0x1f, (byte) 0x47,
            (byte) 0xa0, (byte) 0xd3, (byte) 0xc6, (byte) 0xb9, (byte) 0xe2, (byte) 0xf5, (byte) 0xa8, (byte) 0xd1
    };

    private final SecureRandom random = CryptoServicesRegistrar.getSecureRandom();
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, byte[]> versions = new LinkedHashMap<>();
    private final Map<String, String> createdAt = new LinkedHashMap<>();
    private volatile String currentVersion = INITIAL_VERSION;

    public MockKekClient() {
        versions.put(currentVersion, DEMO_V1_KEY);
        createdAt.put(currentVersion, Instant.now().toString());
    }

    @Override
    public WrapResult wrapDek(byte[] dek) {
        lock.lock();
        try {
            byte[] key = versions.get(currentVersion);
            byte[] nonce = new byte[NONCE_LENGTH];
            random.nextBytes(nonce);
            byte[] wrapped = gcmEncrypt(key, nonce, dek);
            byte[] edek = new byte[nonce.length + wrapped.length];
            System.arraycopy(nonce, 0, edek, 0, nonce.length);
            System.arraycopy(wrapped, 0, edek, nonce.length, wrapped.length);
            return new WrapResult(edek, currentVersion);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] unwrapDek(byte[] edek, String kekVersion) {
        byte[] key = versions.get(kekVersion);
        if (key == null) {
            throw new IllegalArgumentException("Unknown demo KEK version: " + kekVersion);
        }
        byte[] nonce = Arrays.copyOfRange(edek, 0, NONCE_LENGTH);
        byte[] wrapped = Arrays.copyOfRange(edek, NONCE_LENGTH, edek.length);
        return gcmDecrypt(key, nonce, wrapped);
    }

    @Override
    public String getCurrentKekVersion() {
        return currentVersion;
    }

    /** Demo-only: the real Azure client has no such method; the demo HSM stand-in must mint its own new key version. */
    public String rotateToNewVersion() {
        lock.lock();
        try {
            int maxN = 0;
            for (String v : versions.keySet()) {
                if (v.startsWith("demo-v")) {
                    try {
                        maxN = Math.max(maxN, Integer.parseInt(v.substring("demo-v".length())));
                    } catch (NumberFormatException ignored) {
                        // non-numeric suffix, skip
                    }
                }
            }
            String newVersion = "demo-v" + (maxN + 1);
            byte[] newKey = new byte[32];
            random.nextBytes(newKey);
            versions.put(newVersion, newKey);
            createdAt.put(newVersion, Instant.now().toString());
            currentVersion = newVersion;
            return newVersion;
        } finally {
            lock.unlock();
        }
    }

    /** Demo-only introspection for GET /demo/hsm-state. Never exposes raw key bytes. */
    public DemoState getState() {
        List<DemoState.VersionInfo> infos = new ArrayList<>();
        List<String> sorted = new ArrayList<>(versions.keySet());
        sorted.sort(String::compareTo);
        for (String v : sorted) {
            infos.add(new DemoState.VersionInfo(v, createdAt.get(v), v.equals(currentVersion), versions.get(v).length * 8));
        }
        return new DemoState(currentVersion, versions.size(), infos);
    }

    public record DemoState(String currentVersion, int totalVersions, List<VersionInfo> versions) {
        public record VersionInfo(String version, String createdAt, boolean isCurrent, int keyLengthBits) {
        }
    }

    @Override
    public String fetchSecret(String secretName) {
        return "";
    }

    @Override
    public SecretWithVersion fetchSecretWithVersion(String secretName) {
        return new SecretWithVersion("", "");
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
            throw new IllegalStateException("Demo KEK wrap failed", e);
        }
    }

    private static byte[] gcmDecrypt(byte[] key, byte[] nonce, byte[] ciphertext) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION, PROVIDER);
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(TAG_LENGTH_BITS, nonce));
            return cipher.doFinal(ciphertext);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Demo KEK unwrap failed", e);
        }
    }
}
