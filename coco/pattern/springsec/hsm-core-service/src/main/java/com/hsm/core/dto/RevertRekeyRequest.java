package com.hsm.core.dto;

import jakarta.validation.constraints.NotBlank;

public record RevertRekeyRequest(@NotBlank String kekName) {
}
