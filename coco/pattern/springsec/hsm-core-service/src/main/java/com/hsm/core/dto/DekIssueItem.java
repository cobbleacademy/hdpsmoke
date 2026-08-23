package com.hsm.core.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * One item of a POST /dek/issue request. No plaintext travels here at all --
 * unlike EncryptRequest, /dek/issue never sees plaintext; the caller (CLNT)
 * does the AES-GCM encrypt locally after unwrapping the returned wrapped_dek.
 * {@code key} is caller-supplied for correlation, same pattern as BatchEncryptItem.
 *
 * <p>{@code name} is optional and independent of {@code key}: unset -&gt; always mint
 * a fresh DEK for this item (today's default, DEK-per-item). Set -&gt; reuse the
 * current DEK for (appId, name) if one already exists, same semantics as
 * EncryptRequest.dekName.
 */
public record DekIssueItem(
        @NotBlank @Size(max = 256) String key,
        String dataClassification,
        @Size(max = 256) String name
) {
    public DekIssueItem {
        // A blank (not just null) name must normalize to real null -- see
        // EncryptRequest's compact constructor for why: it ends up as
        // edek_records.current_dek_name, uniquely indexed per app_id, and "" colliding
        // with another "" (unlike null vs null) would break every second unnamed item.
        if (name != null && name.isBlank()) {
            name = null;
        }
    }
}
