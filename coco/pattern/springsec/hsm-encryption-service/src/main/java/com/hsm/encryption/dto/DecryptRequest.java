package com.hsm.encryption.dto;

import java.util.UUID;

/**
 * Ported from app/models/schemas.py's DecryptRequest. Provide either the
 * preferred ciphertextToken, or the legacy edekId/ivB64/ciphertextB64/tagB64
 * fields. Cross-field "one or the other" validation and base64 format checks
 * happen in DecryptionService (mirrors Python's model_post_init), not here.
 */
public record DecryptRequest(
        String ciphertextToken,
        UUID edekId,
        String ivB64,
        String ciphertextB64,
        String tagB64,
        String endUserId
) {
}
