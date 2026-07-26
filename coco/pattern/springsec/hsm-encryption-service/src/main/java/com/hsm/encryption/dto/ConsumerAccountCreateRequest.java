package com.hsm.encryption.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/** Demo-only. Ported from app/models/schemas.py's ConsumerAccountCreateRequest. */
public record ConsumerAccountCreateRequest(
        @NotBlank @Size(max = 128) String customerName,
        @NotBlank @Size(max = 256) String email,
        @NotBlank @Size(max = 64) String accountNumber // sensitive -- never stored as-is
) {
}
