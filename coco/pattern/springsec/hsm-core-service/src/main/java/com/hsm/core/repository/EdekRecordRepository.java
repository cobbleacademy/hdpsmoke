package com.hsm.core.repository;

import com.hsm.core.model.EdekRecord;
import com.hsm.core.model.RotationStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface EdekRecordRepository extends JpaRepository<EdekRecord, UUID> {

    /** Used by GET /demo/edek-records. */
    List<EdekRecord> findAllByOrderByCreatedAtDesc(Pageable pageable);

    /** Named-DEK reuse lookup -- at most one row per (appId, dekName) can match, enforced by idx_edek_current_name. */
    Optional<EdekRecord> findByAppIdAndCurrentDekName(String appId, String dekName);

    /** Used by the named-DEK rotation scheduler to find rows past their age threshold. */
    List<EdekRecord> findByRotationStatusAndCurrentDekNameIsNotNullAndCreatedAtBefore(
            RotationStatus rotationStatus, OffsetDateTime cutoff);

    /**
     * RotationService.rotateKek: which distinct KEKs actually have current EDEKs, to sweep -- see V8's migration
     * comment on why this (not kek_registry) is the source of truth for what rotateKek iterates. Explicit @Query
     * (not a derived findDistinctKekNameBy... method name) because the property being projected and the property
     * used in the IsNotNull predicate are both "kekName", which Spring Data's method-name parser cannot
     * disambiguate -- it silently falls back to projecting the whole entity instead of just the column.
     */
    @Query("SELECT DISTINCT e.kekName FROM EdekRecord e WHERE e.rotationStatus = :status AND e.kekName IS NOT NULL")
    List<String> findDistinctKekNamesForCurrentRecords(@Param("status") RotationStatus status);

    /** RotationService.rotateKek: whether any pre-multi-KEK rows (kek_name never backfilled) still need sweeping under the legacy default KEK. */
    boolean existsByRotationStatusAndKekNameIsNull(RotationStatus rotationStatus);

    /** RotationService.rotateKek's per-KEK-group paging loop. */
    Page<EdekRecord> findByRotationStatusAndKekNameAndKekVersionNotOrderByCreatedAtAsc(
            RotationStatus rotationStatus, String kekName, String kekVersion, Pageable pageable);

    /** Same as above, for the legacy-default group whose rows still have kek_name = NULL. */
    Page<EdekRecord> findByRotationStatusAndKekNameIsNullAndKekVersionNotOrderByCreatedAtAsc(
            RotationStatus rotationStatus, String kekVersion, Pageable pageable);

    /** RotationService.rekey's paging loop -- every current row under the source KEK, regardless of its kek_version. */
    Page<EdekRecord> findByRotationStatusAndKekNameOrderByCreatedAtAsc(
            RotationStatus rotationStatus, String kekName, Pageable pageable);

    /** Same as above, for rekeying away from the legacy-default group whose rows still have kek_name = NULL. */
    Page<EdekRecord> findByRotationStatusAndKekNameIsNullOrderByCreatedAtAsc(
            RotationStatus rotationStatus, Pageable pageable);

    /** RotationService.revertRekey's paging loop -- current rows sitting under kekName with an undo buffer to restore. */
    Page<EdekRecord> findByRotationStatusAndKekNameAndPreviousKekNameIsNotNullOrderByCreatedAtAsc(
            RotationStatus rotationStatus, String kekName, Pageable pageable);
}
