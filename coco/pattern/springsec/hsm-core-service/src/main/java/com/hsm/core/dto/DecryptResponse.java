package com.hsm.core.dto;

/** Ported from app/models/schemas.py's DecryptResponse. */
public record DecryptResponse(
        String plaintext,
        String ownerAppId, // the app_id used as AAD when this record was encrypted
        String algorithm,
        String encoding,   // tells the caller how to interpret plaintext (utf8 vs base64)

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
