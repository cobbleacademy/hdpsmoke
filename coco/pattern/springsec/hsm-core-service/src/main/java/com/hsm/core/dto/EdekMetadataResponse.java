package com.hsm.core.dto;

import com.hsm.core.model.RotationStatus;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * GET /admin/edek/{edekId} -- read-only ownership/metadata lookup for support
 * to answer "who owns this EDEK, and under what dek_name" without a DB query
 * or a failed /decrypt attempt. Deliberately excludes edekBlob (wrapped key
 * material) and fingerprint -- this endpoint answers "who owns it," never
 * "what does it decrypt to."
 */
public record EdekMetadataResponse(
        UUID edekId,
        String ownerAppId,
        String dataClassification,
        String algorithm,
        String encoding,
        String kekName,
        String kekVersion,
        RotationStatus rotationStatus,
        String dekName,
        String currentDekName,
        OffsetDateTime createdAt,
        OffsetDateTime rotatedAt
) {
}
