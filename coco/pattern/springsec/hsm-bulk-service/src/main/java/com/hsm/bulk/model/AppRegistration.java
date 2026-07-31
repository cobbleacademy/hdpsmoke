package com.hsm.bulk.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;

/**
 * Duplicated from com.hsm.core.model.AppRegistration, extended with
 * publicKeyPem -- the RSA-OAEP-256 public key hsm-bulk-service transport-wraps
 * every issued/unwrapped DEK with (V6__add_public_key_to_app_registrations.sql).
 * hsm-core-service's own AppRegistration entity intentionally does NOT map this
 * new column -- it never needs it -- but the underlying table has it for either
 * module to read.
 */
@Entity
@Table(name = "app_registrations")
public class AppRegistration {

    @Id
    @Column(name = "app_id", length = 128)
    private String appId;

    @Column(name = "allowed_scopes", nullable = false, length = 512)
    private String allowedScopes;

    @Column(name = "description", nullable = false, length = 512)
    private String description = "";

    @Column(name = "active", nullable = false)
    private boolean active = true;

    @Column(name = "created_at")
    private OffsetDateTime createdAt;

    @Column(name = "updated_at")
    private OffsetDateTime updatedAt;

    @Column(name = "public_key_pem", columnDefinition = "TEXT")
    private String publicKeyPem;

    protected AppRegistration() {
        // JPA
    }

    /** Test/benchmark-only constructor -- production rows are created by hsm-core-service's own onboarding path, not this module. */
    public AppRegistration(String appId, String allowedScopes, String description, boolean active, String publicKeyPem) {
        this.appId = appId;
        this.allowedScopes = allowedScopes;
        this.description = description;
        this.active = active;
        this.publicKeyPem = publicKeyPem;
        this.createdAt = OffsetDateTime.now();
        this.updatedAt = this.createdAt;
    }

    public String getAppId() {
        return appId;
    }

    public String getAllowedScopes() {
        return allowedScopes;
    }

    /** Used by the BulkVsBatchBenchmark PoC harness to grant a demo app dek_issue/dek_unwrap for the test run. */
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

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }

    public String getPublicKeyPem() {
        return publicKeyPem;
    }

    public void setPublicKeyPem(String publicKeyPem) {
        this.publicKeyPem = publicKeyPem;
        this.updatedAt = OffsetDateTime.now();
    }
}
