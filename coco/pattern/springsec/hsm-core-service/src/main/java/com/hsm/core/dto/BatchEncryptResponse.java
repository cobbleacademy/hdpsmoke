package com.hsm.core.dto;

import java.util.List;

public record BatchEncryptResponse(
        List<BatchEncryptResultItem> items
) {
}
