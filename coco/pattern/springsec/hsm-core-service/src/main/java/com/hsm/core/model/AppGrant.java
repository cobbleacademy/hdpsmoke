package com.hsm.core.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Objects;

/**
 * Coarse-grained cross-app authorization: granteeAppId may act (per scope --
 * "encrypt" or "decrypt" today, though this column is unconstrained
 * free-text, same convention as app_registrations.allowed_scopes, so a future
 * third scope needs no schema change) on ANY resource ownerAppId owns for
 * that scope -- any dek_name for encrypt, any EDEK for decrypt. Without a
 * matching row here (or a more specific {@link AppDekGrant} row), an app may
 * only act on its own resources. Replaces the earlier, decrypt-only
 * AppDecryptGrant (V14) -- see that migration's own comment for why it's a
 * replacement, not an extension.
 */
@Entity
@Table(name = "app_grants")
@IdClass(AppGrant.Key.class)
public class AppGrant {

    @jakarta.persistence.Id
    @Column(name = "grantee_app_id", length = 128)
    private String granteeAppId;

    @jakarta.persistence.Id
    @Column(name = "owner_app_id", length = 128)
    private String ownerAppId;

    @jakarta.persistence.Id
    @Column(name = "scope", length = 32)
    private String scope;

    @Column(name = "created_at")
    private OffsetDateTime createdAt;

    protected AppGrant() {
        // JPA
    }

    public AppGrant(String granteeAppId, String ownerAppId, String scope) {
        this.granteeAppId = granteeAppId;
        this.ownerAppId = ownerAppId;
        this.scope = scope;
        this.createdAt = OffsetDateTime.now();
    }

    public String getGranteeAppId() {
        return granteeAppId;
    }

    public String getOwnerAppId() {
        return ownerAppId;
    }

    public String getScope() {
        return scope;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    /** Composite primary key, mirrored by JpaRepository&lt;AppGrant, AppGrant.Key&gt;. */
    public static class Key implements Serializable {
        private String granteeAppId;
        private String ownerAppId;
        private String scope;

        public Key() {
        }

        public Key(String granteeAppId, String ownerAppId, String scope) {
            this.granteeAppId = granteeAppId;
            this.ownerAppId = ownerAppId;
            this.scope = scope;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key key)) return false;
            return Objects.equals(granteeAppId, key.granteeAppId)
                    && Objects.equals(ownerAppId, key.ownerAppId)
                    && Objects.equals(scope, key.scope);
        }

        @Override
        public int hashCode() {
            return Objects.hash(granteeAppId, ownerAppId, scope);
        }
    }
}
