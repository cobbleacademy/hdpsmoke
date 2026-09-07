package com.hsm.core.dto;

import java.util.UUID;

/**
 * {@code ownerAppId} is the record's permanent owner. The caller MUST use
 * this (never its own app_id) as the AES-GCM AAD for its local decrypt,
 * matching what DecryptionService uses server-side for /decrypt -- see
 * DekIssueResultItem's javadoc for the identical reasoning on the encrypt
 * side of this same bug class.
 */
public record DekUnwrapResultItem(
        String key,
        String status,
        UUID edekId,
        String wrappedDekB64,
        String ownerAppId,
        String detail
) {
    public static DekUnwrapResultItem success(String key, UUID edekId, String wrappedDekB64, String ownerAppId) {
        return new DekUnwrapResultItem(key, "success", edekId, wrappedDekB64, ownerAppId, null);
    }

    public static DekUnwrapResultItem error(String key, String detail) {
        return new DekUnwrapResultItem(key, "error", null, null, null, detail);
    }
}
