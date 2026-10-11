package com.hsm.cachekeyrotator;

import com.hsm.crypto.DekCacheCodec;
import com.hsm.crypto.DekManager;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Redis operations on the DEK cache, ported from cek_rotation/redis_ops.py. Key and
 * value formats come from hsm-crypto-core's {@link DekCacheCodec} -- the same codec
 * pinned by java/chassis/test-vectors/crypto-v1.json (contract C7) -- so entries this
 * rewrites stay readable by hsm-core-service's RedisDekCache.
 */
public class RedisOps {

    private static final String KEY_PREFIX = DekCacheCodec.KEY_PREFIX;

    private final RedisCommands<byte[], byte[]> redis;

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
                byte[] newKeyBytes = DekCacheCodec.key(newVersion, edekId);

                Long ttl = redis.ttl(oldKeyBytes);
                byte[] blob = redis.get(oldKeyBytes);
                if (blob == null) {
                    skipped++; // expired between SCAN and GET
                    continue;
                }

                byte[] dek = DekCacheCodec.decode(blob, oldCek);
                byte[] newBlob;
                try {
                    newBlob = DekCacheCodec.encode(dek, newCek);
                } finally {
                    DekManager.zeroDek(dek);
                }

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
}
