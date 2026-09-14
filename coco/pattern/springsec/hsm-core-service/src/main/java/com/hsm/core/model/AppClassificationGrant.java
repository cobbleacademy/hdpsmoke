package com.hsm.core.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Objects;

/**
 * Allow-list entry: appId may declare dataClassification on a freshly minted
 * dek_name. Not a grant between two apps the way {@link AppGrant}/{@link AppDekGrant}
 * are -- there's no "owner" of a classification label -- just a flat
 * (app_id, data_classification) row meaning "approved"; no row means
 * unapproved. See V15's migration comment for the Phase 1 (shadow mode, not
 * yet enforced) / Phase 2 (enforced) rollout this feeds.
 */
@Entity
@Table(name = "app_classification_grants")
@IdClass(AppClassificationGrant.Key.class)
public class AppClassificationGrant {

    @jakarta.persistence.Id
    @Column(name = "app_id", length = 128)
    private String appId;

    @jakarta.persistence.Id
    @Column(name = "data_classification", length = 32)
    private String dataClassification;

    @Column(name = "granted_by", nullable = false, length = 128)
    private String grantedBy;

    @Column(name = "created_at")
    private OffsetDateTime createdAt;

    protected AppClassificationGrant() {
        // JPA
    }

    public AppClassificationGrant(String appId, String dataClassification, String grantedBy) {
        this.appId = appId;
        this.dataClassification = dataClassification;
        this.grantedBy = grantedBy;
        this.createdAt = OffsetDateTime.now();
    }

    public String getAppId() {
        return appId;
    }

    public String getDataClassification() {
        return dataClassification;
    }

    public String getGrantedBy() {
        return grantedBy;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    /** Composite primary key, mirrored by JpaRepository&lt;AppClassificationGrant, AppClassificationGrant.Key&gt;. */
    public static class Key implements Serializable {
        private String appId;
        private String dataClassification;

        public Key() {
        }

        public Key(String appId, String dataClassification) {
            this.appId = appId;
            this.dataClassification = dataClassification;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key key)) return false;
            return Objects.equals(appId, key.appId) && Objects.equals(dataClassification, key.dataClassification);
        }

        @Override
        public int hashCode() {
            return Objects.hash(appId, dataClassification);
        }
    }
}
