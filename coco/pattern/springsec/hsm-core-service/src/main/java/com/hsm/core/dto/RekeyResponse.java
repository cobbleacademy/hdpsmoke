package com.hsm.core.dto;

/**
 * Result of a manual rekey (moving current EDEKs from one kek_name to
 * another) or its reversion. For a reversion, fromKekName is the KEK being
 * reverted away from and toKekName/newKekVersion describe what each record
 * was restored to -- see RotationService.rekey/revertRekey.
 */
public record RekeyResponse(String fromKekName, String toKekName, String newKekVersion, int recordsAffected) {
}
