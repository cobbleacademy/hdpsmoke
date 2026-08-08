package com.hsm.bulk.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Duplicated from com.hsm.core.model.EdekRecord -- maps to the SAME edek_records
 * table hsm-core-service owns and migrates (schema consumer, not owner; see
 * hsm-bulk-service/pom.xml). A record created here via POST /dek/issue is
 * indistinguishable in storage from one created by hsm-core-service's own
 * POST /encrypt -- hsm-core-service's /decrypt endpoint can resolve either
 * without any awareness this module exists.
 */
@Entity
@Table(name = "edek_records")
public class EdekRecord {

    @Id
    @Column(name = "edek_id")
    private UUID edekId;

    @Column(name = "app_id", nullable = false, length = 128)
    private String appId;

    @Column(name = "edek_blob", nullable = false, columnDefinition = "TEXT")
    private String edekBlob;

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

    @Column(name = "fingerprint", length = 16)
    private String fingerprint;

    /** See com.hsm.core.model.EdekRecord's javadoc on the same two fields -- identical semantics, same underlying columns. */
    @Column(name = "dek_name", length = 256)
    private String dekName;

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

    public String getKekVersion() {
        return kekVersion;
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

    public String getDekName() {
        return dekName;
    }

    public String getCurrentDekName() {
        return currentDekName;
    }

    /** Only ever called to backfill a previously-unset classification on a named DEK's first explicit value -- see DekIssueService. */
    public void setDataClassification(String dataClassification) {
        this.dataClassification = dataClassification;
    }

    /** Retires this row from named-DEK reuse eligibility -- see RotationService.rotateNamedDek(). */
    public void clearCurrentDekName() {
        this.currentDekName = null;
    }
}
