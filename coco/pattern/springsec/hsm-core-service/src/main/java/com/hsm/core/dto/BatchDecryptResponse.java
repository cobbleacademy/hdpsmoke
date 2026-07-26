package com.hsm.core.dto;

import java.util.List;

public record BatchDecryptResponse(
        List<BatchDecryptResultItem> items
) {
}
