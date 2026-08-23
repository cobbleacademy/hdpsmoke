package com.hsm.core.dto;

import java.util.List;

public record DekUnwrapResponse(
        List<DekUnwrapResultItem> items
) {
}
