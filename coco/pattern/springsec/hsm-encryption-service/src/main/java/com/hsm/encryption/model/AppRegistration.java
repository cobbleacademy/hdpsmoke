package com.hsm.encryption.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

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

    protected AppRegistration() {
        // JPA
    }

    public AppRegistration(String appId, String allowedScopes, String description, boolean active) {
        this.appId = appId;
        this.allowedScopes = allowedScopes;
        this.description = description;
        this.active = active;
    }

    public String getAppId() {
        return appId;
    }

    public String getAllowedScopes() {
        return allowedScopes;
    }

    public void setAllowedScopes(String allowedScopes) {
        this.allowedScopes = allowedScopes;
    }

    public String getDescription() {
        return description;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }
}
