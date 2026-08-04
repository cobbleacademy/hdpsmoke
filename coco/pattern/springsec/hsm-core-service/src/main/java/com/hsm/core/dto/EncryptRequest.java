package com.hsm.core.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.util.Map;

/** Ported from app/models/schemas.py's EncryptRequest. */
public record EncryptRequest(
        @NotBlank @Size(max = EncryptRequest.MAX_PLAINTEXT_CHARS) String plaintext,
        String encoding,             // "utf8" | "base64" -- how to interpret plaintext on decrypt's way back out
        String dataClassification,   // e.g. "pii", "pci" -- drives audit/retention queries, never enforced here
        String endUserId,            // logged-in user who triggered the call; passed by client for SIEM audit trail
        Map<String, String> context, // caller metadata, stored in audit log only
        @Size(max = 256) String dekName  // optional: reuse the current DEK for (appId, dekName) instead of minting a fresh one -- see EncryptionService.encrypt()
) {
    public static final int MAX_PLAINTEXT_CHARS = 1_048_576; // hard ceiling: 1 MiB characters

    public EncryptRequest {
        if (encoding == null || encoding.isBlank()) {
            encoding = "utf8";
        }
        if (context == null) {
            context = Map.of();
        }
        // A blank (not just null) dekName must normalize to real null -- it ends up as
        // edek_records.current_dek_name, which idx_edek_current_name uniquely indexes
        // per app_id. Storing "" instead of null for every unnamed call would make the
        // SECOND unnamed /encrypt call from the same app collide on that index, since
        // unlike null, "" equals "" under uniqueness.
        if (dekName != null && dekName.isBlank()) {
            dekName = null;
        }
    }
}
