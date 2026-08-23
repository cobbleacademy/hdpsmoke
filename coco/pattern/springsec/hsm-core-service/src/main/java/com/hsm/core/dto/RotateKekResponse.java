package com.hsm.core.dto;

import java.util.List;

/**
 * One entry per distinct kek_name actually swept -- see
 * RotationService.rotateKek and EdekRecordRepository's
 * findDistinctKekNameByRotationStatusAndKekNameIsNotNull for why the KEK set
 * is discovered from edek_records rather than kek_registry.
 */
public record RotateKekResponse(List<KekRotationResult> results, int recordsQueued) {

    public record KekRotationResult(String kekName, String newKekVersion, int recordsRotated) {
    }
}
