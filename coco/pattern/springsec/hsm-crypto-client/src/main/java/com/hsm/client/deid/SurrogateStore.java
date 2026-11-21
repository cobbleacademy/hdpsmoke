package com.hsm.client.deid;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The surrogate table (DEIDENTIFICATION.md section 8): {@code deid_surrogate} in the
 * data owner's database. The bulk client's {@code JdbcSurrogateStore} implements it.
 * Only a profile that uses {@code surrogate} touches it.
 *
 * <p>Implementations do plain reads and inserts. The algorithm (fetch, insert the
 * missing ones, re-read lost races, retry collisions) lives in
 * {@code SurrogateTransform}, so every database behaves the same.
 */
public interface SurrogateStore {

    /** What {@link #insertIfAbsent} did. */
    enum InsertResult {
        /** The row was inserted with the given surrogate. */
        INSERTED,
        /** A row for this (scope, domain, lookup_hash) already exists: another run won the race. */
        ROW_EXISTS,
        /** The surrogate is already used in this (scope, domain): pick another. */
        SURROGATE_TAKEN
    }

    /**
     * Existing surrogates for these lookup hashes.
     *
     * @return surrogate by lookup hash as lower-case hex; hashes with no row are absent
     */
    Map<String, String> find(String scope, String domain, List<byte[]> lookupHashes);

    /** Inserts one row unless its (scope, domain, lookup_hash) or (scope, domain, surrogate) exists. */
    InsertResult insertIfAbsent(String scope, String domain, byte[] lookupHash, byte[] valueEnc, UUID keyEdekId,
                                String surrogate, String runId);

    /** Throws if the table is missing or unusable; called before the first record is read. */
    default void verifyAvailable() {
    }
}
