package com.hsm.core.dto;

import java.time.OffsetDateTime;

public record ClassificationGrantResponse(String appId, String dataClassification, String grantedBy, OffsetDateTime createdAt) {
}
