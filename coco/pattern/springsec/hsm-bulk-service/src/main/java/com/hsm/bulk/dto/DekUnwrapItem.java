package com.hsm.bulk.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.UUID;

public record DekUnwrapItem(
        @NotBlank @Size(max = 256) String key,
        @NotNull UUID edekId
) {
}
