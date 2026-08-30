package com.hsm.core.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;

/** Ported from app/auth/app_registry.py's AppRegistration. Maps app_id to permitted scopes. */
@Entity
@Table(name = "app_registrations")
public class AppRegistration {

    @Id
    @Column(name = "app_id", length = 128)
    private String appId;

    @Column(name = "allowed_scopes", nullable = false, length = 512) // comma-separated
    private String allowedScopes;

    @Column(name = "description", nullable = false, length = 512)
    private String description = "";

    @Column(name = "active", nullable = false)
    private boolean active = true;

    /**
     * PEM-encoded RSA public key used by DekIssueService/DekUnwrapService to
     * transport-wrap a raw DEK for this app -- see TransportWrapper. Nullable:
     * only apps calling /dek/issue or /dek/unwrap need one provisioned.
     */
    @Column(name = "public_key_pem", columnDefinition = "TEXT")
    private String publicKeyPem;

    /**
     * PEM-encoded RSA public key used by SelfSignedAppKeyJwtValidator to verify
     * this app's self-issued bearer JWTs (RFC 7523-style: the caller signs a
     * short-lived assertion locally instead of renewing a token from an
     * external IdP). Nullable -- NULL is the deliberate legacy switch: falls
     * back to publicKeyPem (the DEK-transport key) for signature verification
     * too, for callers that would rather manage one keypair than two. See
     * AppRegistryService.getSigningPublicKey.
     */
    @Column(name = "signing_public_key_pem", columnDefinition = "TEXT")
    private String signingPublicKeyPem;

    /**
     * SHA-256 fingerprint (hex-encoded) of this app's mTLS client certificate,
     * used by MtlsAppIdAuthenticationFilter to accept a mutual-TLS handshake as
     * an alternative to a bearer token. Compared against the fingerprint of
     * whatever certificate was actually presented at the TLS layer, not
     * validated via chain-of-trust -- self-signed certs have no CA to chain to,
     * so identity here is fingerprint-pinned, the same trust shape as SSH
     * host-key pinning. Nullable, with no legacy fallback (unlike
     * signingPublicKeyPem's fallback to publicKeyPem): an app that hasn't
     * registered a cert simply cannot authenticate via mTLS, and falls through
     * to whichever of the other three mechanisms it's configured for.
     */
    @Column(name = "mtls_cert_fingerprint", columnDefinition = "TEXT")
    private String mtlsCertFingerprint;

    @Column(name = "created_at")
    private OffsetDateTime createdAt;

    @Column(name = "updated_at")
    private OffsetDateTime updatedAt;

    protected AppRegistration() {
        // JPA
    }

    public AppRegistration(String appId, String allowedScopes, String description, boolean active) {
        this.appId = appId;
        this.allowedScopes = allowedScopes;
        this.description = description;
        this.active = active;
        this.createdAt = OffsetDateTime.now();
        this.updatedAt = this.createdAt;
    }

    public String getAppId() {
        return appId;
    }

    public String getAllowedScopes() {
        return allowedScopes;
    }

    public void setAllowedScopes(String allowedScopes) {
        this.allowedScopes = allowedScopes;
        this.updatedAt = OffsetDateTime.now();
    }

    public String getDescription() {
        return description;
    }

    public boolean isActive() {
        return active;
    }

    public String getPublicKeyPem() {
        return publicKeyPem;
    }

    public void setPublicKeyPem(String publicKeyPem) {
        this.publicKeyPem = publicKeyPem;
        this.updatedAt = OffsetDateTime.now();
    }

    public String getSigningPublicKeyPem() {
        return signingPublicKeyPem;
    }

    public void setSigningPublicKeyPem(String signingPublicKeyPem) {
        this.signingPublicKeyPem = signingPublicKeyPem;
        this.updatedAt = OffsetDateTime.now();
    }

    public String getMtlsCertFingerprint() {
        return mtlsCertFingerprint;
    }

    public void setMtlsCertFingerprint(String mtlsCertFingerprint) {
        this.mtlsCertFingerprint = mtlsCertFingerprint;
        this.updatedAt = OffsetDateTime.now();
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setActive(boolean active) {
        this.active = active;
        this.updatedAt = OffsetDateTime.now();
    }
}
