package com.hsm.bulk.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * One item of a POST /dek/issue request. No plaintext travels here at all --
 * unlike EncryptRequest, hsm-bulk-service never sees plaintext; the caller (CLNT)
 * does the AES-GCM encrypt locally after unwrapping the returned wrapped_dek.
 * {@code key} is caller-supplied for correlation, same pattern as BatchEncryptItem.
 */
public record DekIssueItem(
        @NotBlank @Size(max = 256) String key,
        String dataClassification
) {
}
