package com.hsm.core.dto;

/**
 * One result of a batch encrypt call, correlated back to the request item
 * by {@code key}. Exactly one of {@code result}/{@code detail} is non-null,
 * selected by {@code status} ("success" | "error").
 */
public record BatchEncryptResultItem(
        String key,
        String status,
        EncryptResponse result,
        String detail
) {
    public static BatchEncryptResultItem success(String key, EncryptResponse result) {
        return new BatchEncryptResultItem(key, "success", result, null);
    }

    public static BatchEncryptResultItem error(String key, String detail) {
        return new BatchEncryptResultItem(key, "error", null, detail);
    }
}
