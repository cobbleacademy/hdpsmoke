package com.hsm.core.dto;

import jakarta.validation.constraints.NotBlank;

/** Fine-grained counterpart to GrantRequest -- authorizes granteeAppId for one specific dekName of ownerAppId's, not all of them. */
public record DekGrantRequest(
        @NotBlank String granteeAppId,
        @NotBlank String ownerAppId,
        @NotBlank String dekName,
        @NotBlank String scope // "encrypt" or "decrypt" -- validated against the known set in AdminController, not here
) {
}
