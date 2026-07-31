package com.hsm.bulk.dto;

import java.util.List;

public record DekUnwrapResponse(
        List<DekUnwrapResultItem> items
) {
}
