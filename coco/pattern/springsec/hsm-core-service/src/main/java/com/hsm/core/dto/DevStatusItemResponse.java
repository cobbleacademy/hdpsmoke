package com.hsm.core.dto;

/** Demo-only. Backs the Development Status tab's editable, DB-persisted rows. */
public record DevStatusItemResponse(
        Long id,
        String category,
        String item,
        String status, // "N" | "P" | "C"
        String notes,
        String updatedAt
) {
}
