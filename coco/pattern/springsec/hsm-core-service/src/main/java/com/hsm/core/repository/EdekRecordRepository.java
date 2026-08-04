package com.hsm.core.repository;

import com.hsm.core.model.EdekRecord;
import com.hsm.core.model.RotationStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface EdekRecordRepository extends JpaRepository<EdekRecord, UUID> {

    /** Used by RotationService.rotateKek's paging loop. */
    Page<EdekRecord> findByRotationStatusAndKekVersionNotOrderByCreatedAtAsc(
            RotationStatus rotationStatus, String kekVersion, Pageable pageable);

    /** Used by GET /demo/edek-records. */
    List<EdekRecord> findAllByOrderByCreatedAtDesc(Pageable pageable);

    /** Named-DEK reuse lookup -- at most one row per (appId, dekName) can match, enforced by idx_edek_current_name. */
    Optional<EdekRecord> findByAppIdAndCurrentDekName(String appId, String dekName);

    /** Used by the named-DEK rotation scheduler to find rows past their age threshold. */
    List<EdekRecord> findByRotationStatusAndCurrentDekNameIsNotNullAndCreatedAtBefore(
            RotationStatus rotationStatus, OffsetDateTime cutoff);
}
