package com.hsm.bulk.dto;

import java.util.UUID;

/**
 * One result of a batch /dek/issue call, correlated back by {@code key}.
 * Exactly one of (edekId+wrappedDekB64) / detail is populated, selected by
 * {@code status} ("success" | "error") -- same partial-failure shape as
 * BatchEncryptResultItem.
 */
public record DekIssueResultItem(
        String key,
        String status,
        UUID edekId,
        String wrappedDekB64,
        String detail,
        boolean reused
) {
    public static DekIssueResultItem success(String key, UUID edekId, String wrappedDekB64, boolean reused) {
        return new DekIssueResultItem(key, "success", edekId, wrappedDekB64, null, reused);
    }

    public static DekIssueResultItem error(String key, String detail) {
        return new DekIssueResultItem(key, "error", null, null, detail, false);
    }
}
