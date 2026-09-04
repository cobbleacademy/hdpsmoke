package com.hsm.core.dto;

import java.time.OffsetDateTime;

public record DekGrantResponse(String granteeAppId, String ownerAppId, String dekName, String scope, OffsetDateTime createdAt) {
}
