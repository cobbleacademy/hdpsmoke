package com.hsm.core.dto;

import java.util.List;

/**
 * One entry per distinct kek_name actually swept -- see
 * RotationService.rotateKek and EdekRecordRepository's
 * findDistinctKekNameByRotationStatusAndKekNameIsNotNull for why the KEK set
 * is discovered from edek_records rather than kek_registry.
 */
public record RotateKekResponse(List<KekRotationResult> results, int recordsQueued) {

    /**
     * status "rotated", or "skipped" when the KEK's latest version can't wrap (expired,
     * disabled, not yet valid) -- skipReason then says which, and no row was touched.
     */
    public record KekRotationResult(String kekName, String newKekVersion, int recordsRotated,
                                    String status, String skipReason) {

        public static KekRotationResult rotated(String kekName, String newKekVersion, int recordsRotated) {
            return new KekRotationResult(kekName, newKekVersion, recordsRotated, "rotated", null);
        }

        public static KekRotationResult skipped(String kekName, String newKekVersion, String skipReason) {
            return new KekRotationResult(kekName, newKekVersion, 0, "skipped", skipReason);
        }

        public boolean wasSkipped() {
            return "skipped".equals(status);
        }
    }
}
