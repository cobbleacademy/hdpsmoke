package com.hsm.core.dto;

/**
 * One result of a batch decrypt call, correlated back by {@code key}.
 * Exactly one of {@code result}/{@code detail} is non-null, selected by
 * {@code status} ("success" | "error").
 */
public record BatchDecryptResultItem(
        String key,
        String status,
        DecryptResponse result,
        String detail
) {
    public static BatchDecryptResultItem success(String key, DecryptResponse result) {
        return new BatchDecryptResultItem(key, "success", result, null);
    }

    public static BatchDecryptResultItem error(String key, String detail) {
        return new BatchDecryptResultItem(key, "error", null, detail);
    }
}
