package com.hsm.core.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

/**
 * The item-count ceiling ({@code hsm.service.batch-max-items}) is enforced in
 * EncryptionService.encryptBatch, not here -- Bean Validation constraint
 * attributes must be compile-time constants, so a config-driven limit can't
 * be expressed as a {@code @Size(max=...)} on {@code items} (same reason
 * EncryptionService.encrypt checks plaintext byte-length itself rather than
 * relying solely on EncryptRequest's char-based @Size).
 */
public record BatchEncryptRequest(
        @NotEmpty @Valid List<BatchEncryptItem> items
) {
}
