package com.hsm.core.dto;

import java.util.UUID;

/** Ported from app/models/schemas.py's EncryptResponse. */
public record EncryptResponse(
        // Preferred: single opaque token, store and echo back as-is
        String ciphertext, // "v1.<base64url(version|edek_id|iv|tag|ciphertext)>"

        // Informational fields -- useful for logging/audit, not needed for decrypt
        UUID edekId,
        String ownerAppId,
        String algorithm,
        String encoding,
        String kekVersion,

        // Deprecated: individual binary fields, kept for backward compatibility.
        // Clients should use ciphertext instead.
        String ivB64,
        String ciphertextB64,
        String tagB64,

        // true when this call reused an existing named DEK (dekName was set and
        // already had a current row) rather than minting a fresh one -- observability/
        // audit signal, not needed for decrypt. Always false when dekName was unset.
        boolean reused,

        // Additive response envelope (requirement 6) -- all existing fields above are
        // unchanged, nothing renamed. Error responses go through GlobalExceptionHandler's
        // separate {"detail": "..."} shape untouched by this; these fields only appear on
        // a successful 2xx body. No referenceId here -- that has nothing to reference
        // until true async processing (requirement 7b) exists.
        String status,
        String code,
        String message,
        String correlationId
) {
}
