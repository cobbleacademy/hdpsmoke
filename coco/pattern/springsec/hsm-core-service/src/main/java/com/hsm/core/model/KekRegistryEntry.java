package com.hsm.core.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Objects;

/**
 * Maps (app_id, dek_name, data_classification) to the actual Key Vault kek_name
 * to use -- see KekRegistryService for the 3-tier resolution this feeds and
 * V8's migration comment for why dekName/dataClassification use "" (not NULL)
 * as their "not set" sentinel.
 *
 * <p>Consulted only at encrypt time, when minting a brand-new EDEK. Once
 * resolved, an EdekRecord carries its own kekName forward permanently --
 * decrypt and rotation never consult this table again, so a later change here
 * only ever affects new writes, never retroactively reinterprets existing
 * ciphertext (see EdekRecord's own javadoc).
 *
 * <p><b>Ownership, precisely:</b> an exact-dek_name row (dekName set,
 * dataClassification == UNSET -- "tier 1" below) ALSO reserves that
 * specific dek_name for this appId, gated by
 * hsm.dek-name-reservation.enforce -- see DekNameReservationService. The
 * other two tiers (dataClassification set, or both UNSET) carry no
 * reservation intent at all, since they don't name a specific dek_name.
 * This was NOT true when this class was first written -- kek_registry
 * existed for three migrations (V11 through V14) as a pure KEK-selection
 * preference table with zero admission-control role, and dek_name ownership
 * was governed entirely by V14's separate first-encrypt-wins mechanism
 * (idx_edek_current_name + app_grants/app_dek_grants). Confirmed via a full
 * re-read of every migration, every call site, and the dev-status history
 * before this comment was corrected -- see AUTHORIZATION.md and V14's own
 * migration comment ("no ownership concept existed at all") for that
 * evidence. Flagging the correction explicitly here since the two concepts
 * being easy to conflate is exactly what caused this comment to need fixing.
 */
@Entity
@Table(name = "kek_registry")
@IdClass(KekRegistryEntry.Key.class)
public class KekRegistryEntry {

    /** Sentinel for "this tier not set" on dekName/dataClassification -- see class javadoc. */
    public static final String UNSET = "";

    @jakarta.persistence.Id
    @Column(name = "app_id", length = 128)
    private String appId;

    @jakarta.persistence.Id
    @Column(name = "dek_name", length = 256)
    private String dekName = UNSET;

    @jakarta.persistence.Id
    @Column(name = "data_classification", length = 32)
    private String dataClassification = UNSET;

    @Column(name = "kek_name", nullable = false, length = 127)
    private String kekName;

    @Column(name = "created_at")
    private OffsetDateTime createdAt;

    @Column(name = "updated_at")
    private OffsetDateTime updatedAt;

    protected KekRegistryEntry() {
        // JPA
    }

    public KekRegistryEntry(String appId, String dekName, String dataClassification, String kekName) {
        this.appId = appId;
        this.dekName = dekName == null ? UNSET : dekName;
        this.dataClassification = dataClassification == null ? UNSET : dataClassification;
        this.kekName = kekName;
        this.createdAt = OffsetDateTime.now();
        this.updatedAt = this.createdAt;
    }

    public String getAppId() {
        return appId;
    }

    public String getDekName() {
        return dekName;
    }

    public String getDataClassification() {
        return dataClassification;
    }

    public String getKekName() {
        return kekName;
    }

    public void setKekName(String kekName) {
        this.kekName = kekName;
        this.updatedAt = OffsetDateTime.now();
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }

    /** Composite primary key, mirrored by JpaRepository&lt;KekRegistryEntry, KekRegistryEntry.Key&gt;. */
    public static class Key implements Serializable {
        private String appId;
        private String dekName = UNSET;
        private String dataClassification = UNSET;

        public Key() {
        }

        public Key(String appId, String dekName, String dataClassification) {
            this.appId = appId;
            this.dekName = dekName == null ? UNSET : dekName;
            this.dataClassification = dataClassification == null ? UNSET : dataClassification;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key key)) return false;
            return Objects.equals(appId, key.appId) && Objects.equals(dekName, key.dekName)
                    && Objects.equals(dataClassification, key.dataClassification);
        }

        @Override
        public int hashCode() {
            return Objects.hash(appId, dekName, dataClassification);
        }
    }
}
