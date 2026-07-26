package com.hsm.encryption.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

import java.io.Serializable;
import java.util.Objects;

/**
 * Ported from app/auth/app_registry.py's AppDecryptGrant.
 * Authorizes granteeAppId to decrypt EDEK records owned by ownerAppId. Without a
 * matching row here, an app may only decrypt data it encrypted itself.
 */
@Entity
@Table(name = "app_decrypt_grants")
@IdClass(AppDecryptGrant.Key.class)
public class AppDecryptGrant {

    @jakarta.persistence.Id
    @Column(name = "grantee_app_id", length = 128)
    private String granteeAppId;

    @jakarta.persistence.Id
    @Column(name = "owner_app_id", length = 128)
    private String ownerAppId;

    protected AppDecryptGrant() {
        // JPA
    }

    public AppDecryptGrant(String granteeAppId, String ownerAppId) {
        this.granteeAppId = granteeAppId;
        this.ownerAppId = ownerAppId;
    }

    public String getGranteeAppId() {
        return granteeAppId;
    }

    public String getOwnerAppId() {
        return ownerAppId;
    }

    /** Composite primary key, mirrored by JpaRepository&lt;AppDecryptGrant, AppDecryptGrant.Key&gt;. */
    public static class Key implements Serializable {
        private String granteeAppId;
        private String ownerAppId;

        public Key() {
        }

        public Key(String granteeAppId, String ownerAppId) {
            this.granteeAppId = granteeAppId;
            this.ownerAppId = ownerAppId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key key)) return false;
            return Objects.equals(granteeAppId, key.granteeAppId) && Objects.equals(ownerAppId, key.ownerAppId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(granteeAppId, ownerAppId);
        }
    }
}
