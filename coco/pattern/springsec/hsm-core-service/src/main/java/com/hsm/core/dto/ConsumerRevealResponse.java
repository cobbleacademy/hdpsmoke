package com.hsm.core.dto;

/** Demo-only. Ported from app/models/schemas.py's ConsumerRevealResponse. */
public record ConsumerRevealResponse(
        Long id,
        String accountNumber // decrypted on demand, never written back to the table
) {
}
