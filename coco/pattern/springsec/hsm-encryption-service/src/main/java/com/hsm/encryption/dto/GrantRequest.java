package com.hsm.encryption.dto;

import jakarta.validation.constraints.NotBlank;

public record GrantRequest(
        @NotBlank String granteeAppId, // the app being granted read access
        @NotBlank String ownerAppId    // the app whose encrypted data may be read
) {
}
