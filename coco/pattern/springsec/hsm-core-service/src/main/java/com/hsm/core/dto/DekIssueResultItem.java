package com.hsm.core.dto;

import java.util.UUID;

/**
 * One result of a batch /dek/issue call, correlated back by {@code key}.
 * Exactly one of (edekId+wrappedDekB64+ownerAppId) / detail is populated,
 * selected by {@code status} ("success" | "error") -- same partial-failure
 * shape as BatchEncryptResultItem.
 *
 * <p>{@code ownerAppId} is the record's permanent owner -- not necessarily
 * this caller, once V14 cross-app encrypt grants are in play. The caller
 * MUST use this (never its own app_id) as the AES-GCM AAD for its local
 * encrypt, exactly matching what EncryptionService uses server-side for the
 * /encrypt path. Omitting this field was a real, confirmed bug: a
 * grant-authorized cross-app reuse of a dek_name via /dek/issue previously
 * gave the caller no way to learn the true owner, so any caller using its
 * own app_id (the obvious, only thing it actually had) produced a token
 * nothing could ever decrypt again -- see EncryptionService.ResolvedDek's
 * javadoc for the full reasoning and how this was reproduced end-to-end.
 */
public record DekIssueResultItem(
        String key,
        String status,
        UUID edekId,
        String wrappedDekB64,
        String ownerAppId,
        String detail,
        boolean reused
) {
    public static DekIssueResultItem success(String key, UUID edekId, String wrappedDekB64, String ownerAppId, boolean reused) {
        return new DekIssueResultItem(key, "success", edekId, wrappedDekB64, ownerAppId, null, reused);
    }

    public static DekIssueResultItem error(String key, String detail) {
        return new DekIssueResultItem(key, "error", null, null, null, detail, false);
    }
}
