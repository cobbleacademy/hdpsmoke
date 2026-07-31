package com.hsm.bulk.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

/** Batch-native from day one -- see java/docs/BULK_OPERATIONS.md's Tier 3 design (unlike Tier 1, which started single-item). */
public record DekIssueRequest(
        @NotEmpty @Valid List<DekIssueItem> items
) {
}
