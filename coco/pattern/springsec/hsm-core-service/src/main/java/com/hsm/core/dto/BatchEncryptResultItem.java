package com.hsm.core.dto;

import com.fasterxml.jackson.annotation.JsonView;
import com.hsm.core.web.ResponseViews;

/**
 * One result of a batch encrypt call, correlated back to the request item
 * by {@code key}. Exactly one of {@code result}/{@code detail} is non-null,
 * selected by {@code status} ("success" | "error"). Every field carries an
 * explicit {@code @JsonView} -- see BatchEncryptResponse's comment.
 */
public record BatchEncryptResultItem(
        @JsonView(ResponseViews.Minimal.class) String key,
        @JsonView(ResponseViews.Minimal.class) String status,
        @JsonView(ResponseViews.Minimal.class) EncryptResponse result,
        @JsonView(ResponseViews.Minimal.class) String detail
) {
    public static BatchEncryptResultItem success(String key, EncryptResponse result) {
        return new BatchEncryptResultItem(key, "success", result, null);
    }

    public static BatchEncryptResultItem error(String key, String detail) {
        return new BatchEncryptResultItem(key, "error", null, detail);
    }
}
