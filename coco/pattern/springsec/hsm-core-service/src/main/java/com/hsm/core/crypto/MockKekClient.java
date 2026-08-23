package com.hsm.core.crypto;

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

    /** Per-kekName version history -- one of these per distinct demo KEK, lazily created on first use. */
    private static final class KeyState {
        final Map<String, byte[]> versions = new LinkedHashMap<>();
        final Map<String, String> createdAt = new LinkedHashMap<>();
        volatile String currentVersion = INITIAL_VERSION;
    }

    private final SecureRandom random = CryptoServicesRegistrar.getSecureRandom();
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, KeyState> keysByName = new LinkedHashMap<>();

    /**
     * Lazily creates a fresh demo key the first time a given kekName is ever
     * referenced -- the real Azure client requires a key to already exist in
     * the vault, but demo mode has no separate provisioning step, so any
     * kek_registry-resolved name "just works" the first time it's used,
     * exactly like the single implicit demo key did before multi-KEK support.
     * Caller must hold {@code lock}.
     */
    private KeyState getOrCreateKeyState(String kekName) {
        return keysByName.computeIfAbsent(kekName, name -> {
            KeyState state = new KeyState();
            state.versions.put(state.currentVersion, DEMO_V1_KEY);
            state.createdAt.put(state.currentVersion, Instant.now().toString());
            return state;
        });
    }

    @Override
    public WrapResult wrapDek(byte[] dek, String kekName) {
        lock.lock();
        try {
            KeyState state = getOrCreateKeyState(kekName);
            byte[] key = state.versions.get(state.currentVersion);
            byte[] nonce = new byte[NONCE_LENGTH];
            random.nextBytes(nonce);
            byte[] wrapped = gcmEncrypt(key, nonce, dek);
            byte[] edek = new byte[nonce.length + wrapped.length];
            System.arraycopy(nonce, 0, edek, 0, nonce.length);
            System.arraycopy(wrapped, 0, edek, nonce.length, wrapped.length);
            return new WrapResult(edek, state.currentVersion);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] unwrapDek(byte[] edek, String kekName, String kekVersion) {
        // Must take the same lock as wrapDek/rotateToNewVersion -- keysByName/its
        // KeyStates are plain LinkedHashMaps, not concurrent-safe collections, and
        // rotateToNewVersion() mutates them under this lock. An unguarded read here
        // was safe only by accident, while every call into this class ran on one
        // thread at a time (today's sequential batch processing); introducing any
        // concurrency (bounded batch parallelism, multiple simultaneous requests)
        // makes this a real, if rare, race with a concurrent rotation.
        byte[] key;
        lock.lock();
        try {
            KeyState state = getOrCreateKeyState(kekName);
            key = state.versions.get(kekVersion);
        } finally {
            lock.unlock();
        }
        if (key == null) {
            throw new IllegalArgumentException("Unknown demo KEK version: " + kekName + "/" + kekVersion);
        }
        byte[] nonce = Arrays.copyOfRange(edek, 0, NONCE_LENGTH);
        byte[] wrapped = Arrays.copyOfRange(edek, NONCE_LENGTH, edek.length);
        return gcmDecrypt(key, nonce, wrapped);
    }

    @Override
    public String getCurrentKekVersion(String kekName) {
        lock.lock();
        try {
            return getOrCreateKeyState(kekName).currentVersion;
        } finally {
            lock.unlock();
        }
    }

    /** Demo-only: the real Azure client has no such method; the demo HSM stand-in must mint its own new key version. */
    public String rotateToNewVersion(String kekName) {
        lock.lock();
        try {
            KeyState state = getOrCreateKeyState(kekName);
            int maxN = 0;
            for (String v : state.versions.keySet()) {
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
            state.versions.put(newVersion, newKey);
            state.createdAt.put(newVersion, Instant.now().toString());
            state.currentVersion = newVersion;
            return newVersion;
        } finally {
            lock.unlock();
        }
    }

    /** Every kekName this demo instance has lazily created a key for so far. */
    public List<String> getKnownKekNames() {
        lock.lock();
        try {
            return new ArrayList<>(keysByName.keySet());
        } finally {
            lock.unlock();
        }
    }

    /** Demo-only introspection for GET /demo/hsm-state. Never exposes raw key bytes. */
    public DemoState getState(String kekName) {
        lock.lock();
        try {
            KeyState state = getOrCreateKeyState(kekName);
            List<DemoState.VersionInfo> infos = new ArrayList<>();
            List<String> sorted = new ArrayList<>(state.versions.keySet());
            sorted.sort(String::compareTo);
            for (String v : sorted) {
                infos.add(new DemoState.VersionInfo(v, state.createdAt.get(v), v.equals(state.currentVersion), state.versions.get(v).length * 8));
            }
            return new DemoState(state.currentVersion, state.versions.size(), infos);
        } finally {
            lock.unlock();
        }
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
