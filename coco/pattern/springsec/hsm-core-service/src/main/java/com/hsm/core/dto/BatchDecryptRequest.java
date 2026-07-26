package com.hsm.core.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

/** See BatchEncryptRequest -- same item-count-cap reasoning applies here. */
public record BatchDecryptRequest(
        @NotEmpty @Valid List<BatchDecryptItem> items
) {
}
