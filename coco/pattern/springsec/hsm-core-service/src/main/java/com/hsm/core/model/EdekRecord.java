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

    public EdekRecord(UUID edekId, String appId, String edekBlob, String kekVersion,
                       String algorithm, String encoding, String dataClassification,
                       String fingerprint, String dekName) {
        this.edekId = edekId;
        this.appId = appId;
        this.edekBlob = edekBlob;
        this.kekVersion = kekVersion;
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
