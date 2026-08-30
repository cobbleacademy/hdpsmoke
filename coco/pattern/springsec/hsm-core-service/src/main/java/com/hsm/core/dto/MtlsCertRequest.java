package com.hsm.core.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * POST /admin/apps/mtls-cert -- registers or rotates an app's mTLS client
 * certificate. certPem is the full PEM-encoded X.509 certificate (not just
 * its public key); AdminController computes and stores its SHA-256
 * fingerprint, not the cert itself -- MtlsAppIdAuthenticationFilter only
 * ever needs to compare fingerprints, never to parse a stored cert back out.
 * The target app_id must already exist (404 otherwise), same
 * "onboarding is a versioned migration, not a live API" stance as
 * POST /admin/apps/keys.
 */
public record MtlsCertRequest(
        @NotBlank String appId,
        @NotBlank String certPem
) {
}
