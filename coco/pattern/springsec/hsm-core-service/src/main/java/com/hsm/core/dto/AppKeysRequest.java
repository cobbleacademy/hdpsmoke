package com.hsm.core.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * POST /admin/apps/keys -- provisions or rotates an app's public key(s).
 * encryptionPublicKeyPem is the DEK-transport-wrap key (TransportWrapper,
 * DekIssueService/DekUnwrapService). signingPublicKeyPem is optional --
 * omit it to leave that key unchanged, or to run the app on the legacy
 * one-keypair fallback (SelfSignedAppKeyJwtValidator verifies against
 * encryptionPublicKeyPem when no dedicated signing key is registered). At
 * least one of the two fields must be present -- validated by AdminController,
 * not here, since "at least one of two optional fields" isn't expressible as
 * a single-field Bean Validation annotation.
 */
public record AppKeysRequest(
        @NotBlank String appId,
        String encryptionPublicKeyPem,
        String signingPublicKeyPem
) {
}
