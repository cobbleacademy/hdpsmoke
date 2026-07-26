package com.hsm.encryption.crypto;

/** Drop-in no-op used when the cache is disabled (demo mode or dek-cache.enabled=false). */
public class NullDekCache implements DekCache {

    @Override
    public byte[] get(String edekId) {
        return null;
    }

    @Override
    public void set(String edekId, byte[] dek, String dataClassification) {
        // no-op
    }

    @Override
    public void delete(String edekId) {
        // no-op
    }

    @Override
    public void rotate(byte[] newCek, String newVersion) {
        // no-op
    }

    @Override
    public String getCurrentVersion() {
        return "null";
    }
}
