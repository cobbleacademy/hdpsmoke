package com.hsm.core.dto;

import java.time.OffsetDateTime;

public record GrantResponse(String granteeAppId, String ownerAppId, OffsetDateTime createdAt) {
}
