package com.hsm.core.dto;

import java.time.OffsetDateTime;

public record AppKeysResponse(
        String appId,
        boolean hasEncryptionKey,
        boolean hasSigningKey,
        OffsetDateTime updatedAt
) {
}
