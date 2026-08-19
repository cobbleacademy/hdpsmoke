package com.hsm.core.dto;

import com.fasterxml.jackson.annotation.JsonView;
import com.hsm.core.web.ResponseViews;

/**
 * One result of a batch decrypt call, correlated back by {@code key}.
 * Exactly one of {@code result}/{@code detail} is non-null, selected by
 * {@code status} ("success" | "error"). Every field carries an explicit
 * {@code @JsonView} -- see BatchEncryptResponse's comment.
 */
public record BatchDecryptResultItem(
        @JsonView(ResponseViews.Minimal.class) String key,
        @JsonView(ResponseViews.Minimal.class) String status,
        @JsonView(ResponseViews.Minimal.class) DecryptResponse result,
        @JsonView(ResponseViews.Minimal.class) String detail
) {
    public static BatchDecryptResultItem success(String key, DecryptResponse result) {
        return new BatchDecryptResultItem(key, "success", result, null);
    }

    public static BatchDecryptResultItem error(String key, String detail) {
        return new BatchDecryptResultItem(key, "error", null, detail);
    }
}
