package com.hsm.core.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.util.Map;

/**
 * One item of a batch encrypt request. {@code key} is a caller-supplied
 * identifier (e.g. the calling app's own row ID) echoed back verbatim in
 * {@link BatchEncryptResultItem} so the caller can correlate results back to
 * its own records without relying on response array position.
 */
public record BatchEncryptItem(
        @NotBlank @Size(max = 256) String key,
        @NotBlank @Size(max = EncryptRequest.MAX_PLAINTEXT_CHARS) String plaintext,
        String encoding,
        String dataClassification,
        String endUserId,
        Map<String, String> context
) {
    public BatchEncryptItem {
        if (encoding == null || encoding.isBlank()) {
            encoding = "utf8";
        }
        if (context == null) {
            context = Map.of();
        }
    }
}
