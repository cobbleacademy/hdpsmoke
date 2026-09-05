package com.hsm.core.dto;

import jakarta.validation.constraints.NotBlank;

public record GrantRequest(
        @NotBlank String granteeAppId, // the app being granted access
        @NotBlank String ownerAppId,   // the app whose resources may be accessed
        @NotBlank String scope         // "encrypt" or "decrypt" -- validated against the known set in AdminController, not here (see its comment)
) {
}
