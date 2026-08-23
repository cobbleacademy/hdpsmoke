package com.hsm.core.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;
import java.util.UUID;

/** Ported from app/models/edek_record.py's EDEKRecord. */
@Entity
@Table(name = "edek_records")
public class EdekRecord {

    @Id
    @Column(name = "edek_id")
    private UUID edekId;

    @Column(name = "app_id", nullable = false, length = 128)
    private String appId;

    @Column(name = "edek_blob", nullable = false, columnDefinition = "TEXT")
    private String edekBlob; // base64-encoded wrapped DEK

    @Column(name = "kek_version", nullable = false, length = 64)
    private String kekVersion;

    /**
     * Which KEK actually wrapped edekBlob -- required, not just informational,
     * once there's more than one KEK: kekVersion alone ("version 3") is
     * meaningless without knowing which key it's a version of. NULL on rows
     * written before this column existed, meaning "the single legacy KEK from
     * static config" -- see KekRegistryService and V8's migration comment.
     */
    @Column(name = "kek_name", length = 127)
    private String kekName;

    /**
     * Single-level undo buffer for "rekey" (moving this EDEK from one KEK to a
     * different one -- compromise response, retroactive isolation changes, key
     * decommissioning), not a multi-row history table. rekey copies this row's
     * pre-rekey kekName/kekVersion/edekBlob here before overwriting them;
     * reversion swaps them back and clears these three columns. See
     * RotationService.rekey/revertRekey.
     */
    @Column(name = "previous_kek_name", length = 127)
    private String previousKekName;

    @Column(name = "previous_kek_version", length = 64)
    private String previousKekVersion;

    @Column(name = "previous_edek_blob", columnDefinition = "TEXT")
    private String previousEdekBlob;

    @Column(name = "algorithm", nullable = false, length = 32)
    private String algorithm = "AES-256-GCM";

    @Column(name = "encoding", nullable = false, length = 16)
    private String encoding = "utf8";

    @Column(name = "data_classification", length = 32)
    private String dataClassification;

    @Column(name = "rotation_status", nullable = false, length = 16)
    private RotationStatus rotationStatus = RotationStatus.CURRENT;

    @Column(name = "created_at")
    private OffsetDateTime createdAt;

    @Column(name = "rotated_at")
    private OffsetDateTime rotatedAt;

    /**
     * First 8 bytes of SHA-256(iv || tag) as 16 hex chars. Nullable so pre-existing
     * records (written before this column existed) still decrypt.
     */
    @Column(name = "fingerprint", length = 16)
    private String fingerprint;

    /**
     * Caller-chosen logical name ("customers.ssn") letting many encrypt calls share
     * one DEK instead of each minting its own -- null for the default per-value
     * issuance path. Kept even after rotation, for audit/history -- unlike
     * currentDekName below, this never gets cleared.
     */
    @Column(name = "dek_name", length = 256)
    private String dekName;

    /**
     * Mirrors dekName only while rotationStatus is CURRENT; nulled out the moment a
     * row rotates away from current. This (not dekName) is what
     * idx_edek_current_name enforces uniqueness on -- see V7's migration comment for
     * why this shadow-column split exists (H2 has no partial-unique-index syntax).
     */
    @Column(name = "current_dek_name", length = 256)
    private String currentDekName;

    protected EdekRecord() {
        // JPA
    }

    public EdekRecord(UUID edekId, String appId, String edekBlob, String kekVersion, String kekName,
                       String algorithm, String encoding, String dataClassification,
                       String fingerprint, String dekName) {
        this.edekId = edekId;
        this.appId = appId;
        this.edekBlob = edekBlob;
        this.kekVersion = kekVersion;
        this.kekName = kekName;
        this.algorithm = algorithm;
        this.encoding = encoding;
        this.dataClassification = dataClassification;
        this.rotationStatus = RotationStatus.CURRENT;
        this.fingerprint = fingerprint;
        this.dekName = dekName;
        this.currentDekName = dekName;
        this.createdAt = OffsetDateTime.now();
    }

    public UUID getEdekId() {
        return edekId;
    }

    public String getAppId() {
        return appId;
    }

    public String getEdekBlob() {
        return edekBlob;
    }

    public void setEdekBlob(String edekBlob) {
        this.edekBlob = edekBlob;
    }

    public String getKekVersion() {
        return kekVersion;
    }

    public void setKekVersion(String kekVersion) {
        this.kekVersion = kekVersion;
    }

    public String getKekName() {
        return kekName;
    }

    public void setKekName(String kekName) {
        this.kekName = kekName;
    }

    public String getPreviousKekName() {
        return previousKekName;
    }

    public String getPreviousKekVersion() {
        return previousKekVersion;
    }

    public String getPreviousEdekBlob() {
        return previousEdekBlob;
    }

    /** Stashes this row's current kekName/kekVersion/edekBlob as the single-level undo buffer, before rekey overwrites them. */
    public void stashCurrentAsPrevious() {
        this.previousKekName = this.kekName;
        this.previousKekVersion = this.kekVersion;
        this.previousEdekBlob = this.edekBlob;
    }

    /** Swaps the stashed previous_* values back into the live columns and clears them -- one level of undo, not a stack. */
    public void restorePreviousAndClear() {
        this.kekName = this.previousKekName;
        this.kekVersion = this.previousKekVersion;
        this.edekBlob = this.previousEdekBlob;
        this.previousKekName = null;
        this.previousKekVersion = null;
        this.previousEdekBlob = null;
    }

    /** True only after a rekey has stashed something to revert to. */
    public boolean hasPreviousKekState() {
        return previousKekName != null;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public String getEncoding() {
        return encoding;
    }

    public String getDataClassification() {
        return dataClassification;
    }

    /** Only ever called to backfill a previously-unset classification on a named DEK's first explicit value -- see EncryptionService.resolveDek. */
    public void setDataClassification(String dataClassification) {
        this.dataClassification = dataClassification;
    }

    public RotationStatus getRotationStatus() {
        return rotationStatus;
    }

    public void setRotationStatus(RotationStatus rotationStatus) {
        this.rotationStatus = rotationStatus;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getRotatedAt() {
        return rotatedAt;
    }

    public void setRotatedAt(OffsetDateTime rotatedAt) {
        this.rotatedAt = rotatedAt;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    public void setFingerprint(String fingerprint) {
        this.fingerprint = fingerprint;
    }

    public String getDekName() {
        return dekName;
    }

    public String getCurrentDekName() {
        return currentDekName;
    }

    /** Call when this row rotates away from CURRENT -- clears the uniqueness-enforcing shadow column while leaving dekName (history) intact. */
    public void clearCurrentDekName() {
        this.currentDekName = null;
    }
}
