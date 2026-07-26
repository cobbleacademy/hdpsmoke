package com.hsm.core.repository;

import com.hsm.core.model.EdekRecord;
import com.hsm.core.model.RotationStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface EdekRecordRepository extends JpaRepository<EdekRecord, UUID> {

    /** Used by RotationService.rotateKek's paging loop. */
    Page<EdekRecord> findByRotationStatusAndKekVersionNotOrderByCreatedAtAsc(
            RotationStatus rotationStatus, String kekVersion, Pageable pageable);

    /** Used by GET /demo/edek-records. */
    List<EdekRecord> findAllByOrderByCreatedAtDesc(Pageable pageable);
}
