package com.hsm.core.crypto;

import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;
import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Redis-backed DEK cache with versioned CEK support. Ported from app/crypto/dek_cache.py.
 *
 * <p>After Key Vault unwraps a DEK, the raw bytes are AES-256-GCM encrypted with a
 * Cache Encryption Key (CEK) and stored in Redis for a short TTL window. The CEK
 * is versioned; Redis keys are namespaced {@code dek:{cek_version}:{edek_id}}.
 * On rotation (detected by a background poll), {@link #rotate} atomically promotes
 * current-&gt;prev and installs the new CEK as current. During the transition window,
 * {@link #get} tries current first, falls back to prev on miss, and backfills a
 * prev-hit under the current version so the next read takes the fast path.
 *
 * <p>The DEK is never stored as plaintext. Redis value format: {@code iv_b64:ciphertext_b64}.
 */
public class RedisDekCache implements DekCache {

    private static final String CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String PROVIDER = BouncyCastleFipsProvider.PROVIDER_NAME;
    private static final int TAG_LENGTH_BITS = 128;
    private static final int NONCE_LENGTH = 12;

    private final RedisCommands<byte[], byte[]> redis;
    private final long ttlSeconds;
    private final Set<String> excludedClassifications;
    private final SecureRandom random = CryptoServicesRegistrar.getSecureRandom();
    private final ReentrantLock lock = new ReentrantLock();

    private volatile String currentVersion;
    private volatile byte[] currentCek;
    private volatile String prevVersion;
    private volatile byte[] prevCek;

    public RedisDekCache(
            RedisCommands<byte[], byte[]> redis,
            byte[] cek,
            String version,
            long ttlSeconds,
            Set<String> excludedClassifications,
            byte[] prevCek,
            String prevVersion
    ) {
        if (cek.length != 32) {
            throw new IllegalArgumentException("CEK must be exactly 32 bytes, got " + cek.length);
        }
        if (prevCek != null && prevCek.length != 32) {
            throw new IllegalArgumentException("prev_cek must be exactly 32 bytes, got " + prevCek.length);
        }
        this.redis = redis;
        this.currentVersion = version;
        this.currentCek = cek;
        this.ttlSeconds = ttlSeconds;
        this.excludedClassifications = excludedClassifications;
        this.prevCek = prevCek;
        this.prevVersion = prevVersion;
    }

    @Override
    public String getCurrentVersion() {
        return currentVersion;
    }

    @Override
    public void rotate(byte[] newCek, String newVersion) {
        lock.lock();
        try {
            if (newVersion.equals(currentVersion)) {
                return;
            }
            this.prevVersion = this.currentVersion;
            this.prevCek = this.currentCek;
            this.currentVersion = newVersion;
            this.currentCek = newCek;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] get(String edekId) {
        try {
            byte[] blob = redis.get(keyBytes(currentVersion, edekId));
            if (blob != null) {
                return decryptBlob(blob, currentCek);
            }

            // Grace-period fallback: entry was written by a pod still on the prev CEK.
            String snapshotPrevVersion = prevVersion;
            byte[] snapshotPrevCek = prevCek;
            if (snapshotPrevCek != null && snapshotPrevVersion != null) {
                byte[] prevBlob = redis.get(keyBytes(snapshotPrevVersion, edekId));
                if (prevBlob != null) {
                    byte[] dek = decryptBlob(prevBlob, snapshotPrevCek);
                    writeBlob(edekId, dek); // backfill under current version -> next read is a fast-path hit
                    return dek;
                }
            }
            return null;
        } catch (Exception e) {
            return null; // cache miss on any error -- never block the decrypt path
        }
    }

    @Override
    public void set(String edekId, byte[] dek, String dataClassification) {
        if (dataClassification != null && excludedClassifications.contains(dataClassification.toLowerCase())) {
            return;
        }
        writeBlob(edekId, dek);
    }

    private void writeBlob(String edekId, byte[] dek) {
        try {
            byte[] blob = encryptBlob(dek, currentCek);
            redis.set(keyBytes(currentVersion, edekId), blob, SetArgs.Builder.ex(ttlSeconds));
        } catch (Exception e) {
            // cache write failure is non-fatal
        }
    }

    @Override
    public void delete(String edekId) {
        try {
            if (prevVersion != null) {
                redis.del(keyBytes(currentVersion, edekId), keyBytes(prevVersion, edekId));
            } else {
                redis.del(keyBytes(currentVersion, edekId));
            }
        } catch (Exception e) {
            // ignore
        }
    }

    private static byte[] keyBytes(String version, String edekId) {
        return ("dek:" + version + ":" + edekId).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] encryptBlob(byte[] dek, byte[] cek) {
        byte[] iv = new byte[NONCE_LENGTH];
        random.nextBytes(iv);
        byte[] ciphertext = gcm(Cipher.ENCRYPT_MODE, cek, iv, dek);
        String encoded = Base64.getEncoder().encodeToString(iv) + ":" + Base64.getEncoder().encodeToString(ciphertext);
        return encoded.getBytes(StandardCharsets.US_ASCII);
    }

    private byte[] decryptBlob(byte[] blob, byte[] cek) {
        String s = new String(blob, StandardCharsets.US_ASCII);
        int idx = s.indexOf(':');
        byte[] iv = Base64.getDecoder().decode(s.substring(0, idx));
        byte[] ciphertext = Base64.getDecoder().decode(s.substring(idx + 1));
        return gcm(Cipher.DECRYPT_MODE, cek, iv, ciphertext);
    }

    private byte[] gcm(int mode, byte[] key, byte[] iv, byte[] input) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORMATION, PROVIDER);
            cipher.init(mode, new SecretKeySpec(key, "AES"), new GCMParameterSpec(TAG_LENGTH_BITS, iv));
            return cipher.doFinal(input);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("DEK cache AES-GCM operation failed", e);
        }
    }
}
