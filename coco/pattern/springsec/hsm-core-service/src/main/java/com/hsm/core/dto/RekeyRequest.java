package com.hsm.core.dto;

import jakarta.validation.constraints.NotBlank;

public record RekeyRequest(@NotBlank String fromKekName, @NotBlank String toKekName) {
}
