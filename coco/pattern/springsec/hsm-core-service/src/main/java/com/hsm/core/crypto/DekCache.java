package com.hsm.core.crypto;

/**
 * Redis-backed cache of unwrapped DEK bytes, keyed by edek_id, CEK-encrypted
 * before storage. Implemented by {@link RedisDekCache} when enabled and by
 * {@link NullDekCache} (always-miss) when disabled or in demo mode.
 */
public interface DekCache {

    /** Returns raw DEK bytes on cache hit, null on miss or any Redis error. */
    byte[] get(String edekId);

    /** Encrypt dek with the current CEK and store it. Implementations may skip excluded classifications. */
    void set(String edekId, byte[] dek, String dataClassification);

    /** Explicitly evict cached DEK entries across both current and previous CEK versions. */
    void delete(String edekId);

    /** Atomically promote current CEK -&gt; prev and install newCek as current. */
    void rotate(byte[] newCek, String newVersion);

    String getCurrentVersion();
}
