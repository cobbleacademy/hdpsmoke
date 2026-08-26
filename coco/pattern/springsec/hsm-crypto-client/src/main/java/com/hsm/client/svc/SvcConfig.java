package com.hsm.client.svc;

/**
 * Configuration for SvcClient's calls to SVC's POST /dek/issue and
 * POST /dek/unwrap. Deliberately a plain record, not annotated with any
 * Spring binding -- this module has no Spring dependency at all, so an
 * embedding application (or hsm-bulk-client's own Spring
 * {@code @ConfigurationProperties} binding, which composes this type as its
 * {@code svc} field) constructs it directly.
 */
public record SvcConfig(
        String baseUrl,
        String apiV1Prefix,    // must match SVC's own API_V1_PREFIX (hsm.service.api-v1-prefix) -- the two are configured independently and not auto-synced
        String appId,
        AuthMode authMode,     // STATIC (default) uses token below, unchanged; AZURE_AD acquires a fresh bearer token per call via Workload Identity -- see AzureAdTokenProvider; SELF_SIGNED_JWT locally signs a short-lived assertion instead -- see SelfSignedJwtTokenProvider
        String token,          // only used when authMode=STATIC -- a real Azure AD JWT here would expire mid-job on any run longer than its TTL
        String azureTokenScope, // only used when authMode=AZURE_AD -- must match whatever SVC's own Azure AD app registration exposes as its audience/scope
        int dekBatchMaxItems,  // mirrors hsm.service.dek-batch-max-items on SVC -- self-limit client-side rather than rely on SVC's 422 rejection
        String privateKeyPem,  // PKCS#8 PEM, the private half of the public key registered on app_registrations.public_key_pem for appId -- never sent anywhere, only used locally to unwrap what SVC returns
        // Only used when authMode=SELF_SIGNED_JWT -- PKCS#8 PEM, the private half of
        // the key registered on app_registrations.signing_public_key_pem (or
        // public_key_pem, if this app runs on the legacy one-keypair fallback -- SVC's
        // SelfSignedAppKeyJwtValidator resolves that fallback server-side; this field
        // just needs to match whichever key ends up registered). Deliberately a
        // separate field from privateKeyPem above even for legacy one-keypair callers
        // (set both to the same PEM in that case) -- keeps the two purposes
        // (DEK-transport unwrap vs. request signing) textually distinct in config even
        // when the underlying key material is shared.
        String signingPrivateKeyPem,
        // Only used when authMode=SELF_SIGNED_JWT -- the assertion's aud claim. Must
        // match SVC's own hsm.jwt.audience or SelfSignedAppKeyJwtValidator rejects it.
        // Blank/unset defaults to "hsm-core-service", SVC's own default.
        String selfSignedAudience
) {
    public SvcConfig {
        if (selfSignedAudience == null || selfSignedAudience.isBlank()) {
            selfSignedAudience = "hsm-core-service";
        }
    }

    public enum AuthMode { STATIC, AZURE_AD, SELF_SIGNED_JWT }
}
