package com.hsm.encryption.dto;

import java.util.UUID;

/** Ported from app/models/schemas.py's EncryptResponse. */
public record EncryptResponse(
        // Preferred: single opaque token, store and echo back as-is
        String ciphertextToken, // "v1.<base64url(version|edek_id|iv|tag|ciphertext)>"

        // Informational fields -- useful for logging/audit, not needed for decrypt
        UUID edekId,
        String ownerAppId,
        String algorithm,
        String encoding,
        String kekVersion,

        // Deprecated: individual binary fields, kept for backward compatibility.
        // Clients should use ciphertextToken instead.
        String ivB64,
        String ciphertextB64,
        String tagB64
) {
}
