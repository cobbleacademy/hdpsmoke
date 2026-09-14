package com.hsm.core.dto;

import java.time.OffsetDateTime;

public record KekRegistryEntryResponse(
        String appId, String dekName, String dataClassification, String kekName,
        OffsetDateTime createdAt, OffsetDateTime updatedAt
) {
}
