package com.hsm.core.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.util.UUID;

/**
 * One item of a batch decrypt request. {@code key} is a caller-supplied
 * identifier echoed back in {@link BatchDecryptResultItem} for correlation.
 * Provide either {@code ciphertextToken}, or the legacy
 * {@code edekId}/{@code ivB64}/{@code ciphertextB64}/{@code tagB64} fields --
 * same either-or contract as {@link DecryptRequest}. The check itself lives
 * in {@code DecryptionService.decrypt}, not here, so a malformed item
 * surfaces as that item's error rather than a whole-batch validation
 * failure (unlike batch encrypt's plaintext checks, which are enforceable
 * with static Bean Validation annotations and so reject the whole request).
 */
public record BatchDecryptItem(
        @NotBlank @Size(max = 256) String key,
        String ciphertextToken,
        UUID edekId,
        String ivB64,
        String ciphertextB64,
        String tagB64,
        String endUserId
) {
}
