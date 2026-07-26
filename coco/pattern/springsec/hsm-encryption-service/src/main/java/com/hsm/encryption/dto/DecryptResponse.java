package com.hsm.encryption.dto;

/** Ported from app/models/schemas.py's DecryptResponse. */
public record DecryptResponse(
        String plaintext,
        String ownerAppId, // the app_id used as AAD when this record was encrypted
        String algorithm,
        String encoding    // tells the caller how to interpret plaintext (utf8 vs base64)
) {
}
