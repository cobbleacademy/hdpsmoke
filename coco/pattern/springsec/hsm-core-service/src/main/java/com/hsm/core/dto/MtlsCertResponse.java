package com.hsm.core.dto;

import java.time.OffsetDateTime;

/** Response for POST /admin/apps/mtls-cert -- the fingerprint is returned so the caller can confirm what got stored without re-deriving it themselves. */
public record MtlsCertResponse(String appId, String fingerprint, OffsetDateTime updatedAt) {
}
