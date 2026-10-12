package com.hsm.core.crypto;

import com.hsm.crypto.DekCacheCodec;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Redis-backed DEK cache with versioned CEK support. Ported from app/crypto/dek_cache.py.
 *
 * <p>After Key Vault unwraps a DEK, the raw bytes are AES-256-GCM encrypted with a
 * Cache Encryption Key (CEK) and stored in Redis for a short TTL window. The CEK
 * is versioned; Redis keys are namespaced {@code dek:{slot}:{kv_version}:{edek_id}}.
 * On rotation (detected by a background poll), {@link #rotate} atomically promotes
 * current-&gt;prev and installs the new CEK as current. During the transition window,
 * {@link #get} tries current first, falls back to prev on miss, and backfills a
 * prev-hit under the current version so the next read takes the fast path.
 *
 * <p>The DEK is never stored as plaintext. Key and value formats come from hsm-crypto-core's
 * {@link DekCacheCodec} -- the same codec hsm-cache-key-rotator rekeys with, pinned by
 * java/chassis/test-vectors/crypto-v1.json (contract C7).
 */
public class RedisDekCache implements DekCache {

    private final RedisCommands<byte[], byte[]> redis;
    private final long ttlSeconds;
    private final Set<String> excludedClassifications;
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
            byte[] blob = redis.get(DekCacheCodec.key(currentVersion, edekId));
            if (blob != null) {
                return DekCacheCodec.decode(blob, currentCek);
            }

            // Grace-period fallback: entry was written by a pod still on the prev CEK.
            String snapshotPrevVersion = prevVersion;
            byte[] snapshotPrevCek = prevCek;
            if (snapshotPrevCek != null && snapshotPrevVersion != null) {
                byte[] prevBlob = redis.get(DekCacheCodec.key(snapshotPrevVersion, edekId));
                if (prevBlob != null) {
                    byte[] dek = DekCacheCodec.decode(prevBlob, snapshotPrevCek);
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
            byte[] blob = DekCacheCodec.encode(dek, currentCek);
            redis.set(DekCacheCodec.key(currentVersion, edekId), blob, SetArgs.Builder.ex(ttlSeconds));
        } catch (Exception e) {
            // cache write failure is non-fatal
        }
    }

    @Override
    public void delete(String edekId) {
        try {
            if (prevVersion != null) {
                redis.del(DekCacheCodec.key(currentVersion, edekId), DekCacheCodec.key(prevVersion, edekId));
            } else {
                redis.del(DekCacheCodec.key(currentVersion, edekId));
            }
        } catch (Exception e) {
            // ignore
        }
    }
}
