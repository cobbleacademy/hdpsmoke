package com.hsm.cekrotation;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Redis key scheme and operations, ported from cek_rotation/redis_ops.py.
 * Key format: {@code dek:{slot}:{kv_version}:{edek_id}}, value format:
 * {@code iv_b64:ciphertext_b64} (AES-256-GCM, no AAD) -- matches the main
 * service's RedisDekCache exactly.
 */
public class RedisOps {

    private static final String KEY_PREFIX = "dek:";
    private static final String CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String PROVIDER = BouncyCastleFipsProvider.PROVIDER_NAME;
    private static final int TAG_LENGTH_BITS = 128;
    private static final int NONCE_LENGTH = 12;

    private final RedisCommands<byte[], byte[]> redis;
    private final SecureRandom random = CryptoServicesRegistrar.getSecureRandom();

    public RedisOps(RedisCommands<byte[], byte[]> redis) {
        this.redis = redis;
    }

    public record RekeyResult(int rekeyed, int skipped, int failed) {
    }

    /** SCANs dek:* and counts entries per "{slot}:{kv_version}" composite. */
    public Map<String, Integer> countByVersion() {
        Map<String, Integer> counts = new HashMap<>();
        for (byte[] keyBytes : scanKeys(KEY_PREFIX + "*")) {
            String key = new String(keyBytes, StandardCharsets.UTF_8);
            String[] parts = key.split(":", 4);
            if (parts.length == 4) {
                String versionKey = parts[1] + ":" + parts[2];
                counts.merge(versionKey, 1, Integer::sum);
            }
        }
        return counts;
    }

    /** SCANs and deletes all dek:* keys. Returns the count deleted. */
    public int flushDekCache() {
        List<byte[]> keys = scanKeys(KEY_PREFIX + "*");
        if (keys.isEmpty()) {
            return 0;
        }
        redis.del(keys.toArray(new byte[0][]));
        return keys.size();
    }

    /**
     * Migrates every {@code dek:{oldVersion}:*} entry to {@code dek:{newVersion}:*},
     * decrypting with oldCek and re-encrypting with newCek (fresh IV per entry).
     * Per-key best-effort: a failure on one entry increments failed and continues.
     */
    public RekeyResult rekeyDekCache(byte[] oldCek, byte[] newCek, String oldVersion, String newVersion, long defaultTtlSeconds) {
        int rekeyed = 0;
        int skipped = 0;
        int failed = 0;
        String oldPrefix = KEY_PREFIX + oldVersion + ":";
        for (byte[] oldKeyBytes : scanKeys(oldPrefix + "*")) {
            try {
                String oldKey = new String(oldKeyBytes, StandardCharsets.UTF_8);
                String edekId = oldKey.substring(oldPrefix.length());
                byte[] newKeyBytes = (KEY_PREFIX + newVersion + ":" + edekId).getBytes(StandardCharsets.UTF_8);

                Long ttl = redis.ttl(oldKeyBytes);
                byte[] blob = redis.get(oldKeyBytes);
                if (blob == null) {
                    skipped++; // expired between SCAN and GET
                    continue;
                }

                byte[] plaintext = decryptBlob(blob, oldCek);
                byte[] newIv = new byte[NONCE_LENGTH];
                random.nextBytes(newIv);
                byte[] newBlob = encryptBlob(plaintext, newCek, newIv);

                long remaining = (ttl != null && ttl > 0) ? ttl : defaultTtlSeconds;
                redis.set(newKeyBytes, newBlob, SetArgs.Builder.ex(remaining));
                redis.del(oldKeyBytes);
                rekeyed++;
            } catch (Exception e) {
                failed++;
            }
        }
        return new RekeyResult(rekeyed, skipped, failed);
    }

    private List<byte[]> scanKeys(String pattern) {
        List<byte[]> keys = new ArrayList<>();
        ScanArgs args = ScanArgs.Builder.matches(pattern).limit(500);
        ScanCursor cursor = ScanCursor.INITIAL;
        do {
            KeyScanCursor<byte[]> result = redis.scan(cursor, args);
            keys.addAll(result.getKeys());
            cursor = result;
        } while (!cursor.isFinished());
        return keys;
    }

    private byte[] encryptBlob(byte[] dek, byte[] cek, byte[] iv) {
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
            throw new IllegalStateException("CEK rotation AES-GCM operation failed", e);
        }
    }
}
