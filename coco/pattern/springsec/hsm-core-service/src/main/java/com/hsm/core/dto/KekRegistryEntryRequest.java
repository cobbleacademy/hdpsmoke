package com.hsm.core.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * dekName/dataClassification are optional (null/blank -> the UNSET tier
 * sentinel, see KekRegistryEntry) -- omit both for a per-app default row,
 * omit only dekName for a classification-tier row, or set dekName for an
 * exact-dek_name (tier 1) row. kekName is required for POST, ignored for
 * DELETE (validated in KekRegistryService.addOrUpdateEntry, not here, since
 * DELETE reuses this same request shape and has no use for it).
 */
public record KekRegistryEntryRequest(
        @NotBlank String appId,
        String dekName,
        String dataClassification,
        String kekName
) {
}
