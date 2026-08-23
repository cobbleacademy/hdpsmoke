package com.hsm.core.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public record DekUnwrapRequest(
        @NotEmpty @Valid List<DekUnwrapItem> items
) {
}
