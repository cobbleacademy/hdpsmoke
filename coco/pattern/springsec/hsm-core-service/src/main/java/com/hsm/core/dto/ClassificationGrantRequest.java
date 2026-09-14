package com.hsm.core.dto;

import jakarta.validation.constraints.NotBlank;

public record ClassificationGrantRequest(
        @NotBlank String appId,             // the app being approved to use this classification
        @NotBlank String dataClassification // e.g. "pii", "pci" -- free text, matches EncryptRequest.dataClassification's own convention (see AdminController's comment on why this isn't a fixed enum here either)
) {
}
