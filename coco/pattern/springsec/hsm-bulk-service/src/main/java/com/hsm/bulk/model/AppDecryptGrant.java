package com.hsm.bulk.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Objects;

/**
 * Duplicated from com.hsm.core.model.AppDecryptGrant -- POST /dek/unwrap enforces
 * the same owner/grant check as hsm-core-service's POST /decrypt, reading the
 * same app_decrypt_grants table.
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

    @Column(name = "created_at")
    private OffsetDateTime createdAt;

    protected AppDecryptGrant() {
        // JPA
    }

    public AppDecryptGrant(String granteeAppId, String ownerAppId) {
        this.granteeAppId = granteeAppId;
        this.ownerAppId = ownerAppId;
        this.createdAt = OffsetDateTime.now();
    }

    public String getGranteeAppId() {
        return granteeAppId;
    }

    public String getOwnerAppId() {
        return ownerAppId;
    }

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
