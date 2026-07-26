package com.hsm.core.dto;

import jakarta.validation.constraints.NotBlank;

/** Demo-only. Ported from app/models/schemas.py's ConsumerRevealRequest. */
public record ConsumerRevealRequest(
        @NotBlank String revealAs, // which app_id is asking to decrypt -- exercises the same grant model as /decrypt
        String endUserId           // logged-in user who triggered the reveal; passed through to audit log
) {
}
