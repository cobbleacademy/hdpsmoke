package com.hsm.core.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Objects;

/**
 * Fine-grained cross-app authorization: granteeAppId may act (per scope) on
 * SPECIFICALLY this one dekName of ownerAppId's -- not any of ownerAppId's
 * other resources. Checked only when no {@link AppGrant} (coarse) row
 * already covers the same (granteeAppId, ownerAppId, scope) -- see
 * AppRegistryService.isGranted. No equivalent to this fine-grained option
 * existed for decrypt before V14 (app_decrypt_grants was coarse-only); this
 * table now covers both scopes, a deliberate improvement over the old
 * decrypt-only model, not just parity with the new encrypt-side need.
 */
@Entity
@Table(name = "app_dek_grants")
@IdClass(AppDekGrant.Key.class)
public class AppDekGrant {

    @jakarta.persistence.Id
    @Column(name = "grantee_app_id", length = 128)
    private String granteeAppId;

    @jakarta.persistence.Id
    @Column(name = "owner_app_id", length = 128)
    private String ownerAppId;

    @jakarta.persistence.Id
    @Column(name = "dek_name", length = 256)
    private String dekName;

    @jakarta.persistence.Id
    @Column(name = "scope", length = 32)
    private String scope;

    @Column(name = "created_at")
    private OffsetDateTime createdAt;

    protected AppDekGrant() {
        // JPA
    }

    public AppDekGrant(String granteeAppId, String ownerAppId, String dekName, String scope) {
        this.granteeAppId = granteeAppId;
        this.ownerAppId = ownerAppId;
        this.dekName = dekName;
        this.scope = scope;
        this.createdAt = OffsetDateTime.now();
    }

    public String getGranteeAppId() {
        return granteeAppId;
    }

    public String getOwnerAppId() {
        return ownerAppId;
    }

    public String getDekName() {
        return dekName;
    }

    public String getScope() {
        return scope;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    /** Composite primary key, mirrored by JpaRepository&lt;AppDekGrant, AppDekGrant.Key&gt;. */
    public static class Key implements Serializable {
        private String granteeAppId;
        private String ownerAppId;
        private String dekName;
        private String scope;

        public Key() {
        }

        public Key(String granteeAppId, String ownerAppId, String dekName, String scope) {
            this.granteeAppId = granteeAppId;
            this.ownerAppId = ownerAppId;
            this.dekName = dekName;
            this.scope = scope;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key key)) return false;
            return Objects.equals(granteeAppId, key.granteeAppId)
                    && Objects.equals(ownerAppId, key.ownerAppId)
                    && Objects.equals(dekName, key.dekName)
                    && Objects.equals(scope, key.scope);
        }

        @Override
        public int hashCode() {
            return Objects.hash(granteeAppId, ownerAppId, dekName, scope);
        }
    }
}
