package com.hsm.core.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/** Demo-only. Full replace of one Development Status row's editable fields. */
public record DevStatusUpdateRequest(
        @NotBlank @Size(max = 64) String category,
        @NotBlank @Size(max = 512) String item,
        @NotBlank @Pattern(regexp = "N|P|C") String status,
        @Size(max = 1024) String notes
) {
}
