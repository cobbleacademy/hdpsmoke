package com.hsm.core.dto;

/** Demo-only. Ported from app/models/schemas.py's ConsumerAccountResponse. */
public record ConsumerAccountResponse(
        Long id,
        String customerName,     // non-sensitive
        String email,            // non-sensitive
        String ciphertextToken,  // opaque token -- store and echo back to /decrypt; never decode client-side
        String createdAt
) {
}
