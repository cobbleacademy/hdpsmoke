package com.hsm.core.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/** Demo-only. Adds a new row to the Development Status tab. */
public record DevStatusCreateRequest(
        @NotBlank @Size(max = 64) String category,
        @NotBlank @Size(max = 512) String item,
        @NotBlank @Pattern(regexp = "N|P|C") String status,
        @Size(max = 1024) String notes
) {
}
