package com.hsm.bulk.service;

import com.hsm.bulk.audit.AuditLogger;
import com.hsm.bulk.crypto.DekManager;
import com.hsm.bulk.crypto.KekClient;
import com.hsm.bulk.model.EdekRecord;
import com.hsm.bulk.model.RotationStatus;
import com.hsm.bulk.repository.EdekRecordRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

/**
 * Mirrors com.hsm.core.service.RotationService.rotateNamedDeks/rotateNamedDek
 * exactly -- same age-based sweep, same one-row-per-transaction isolation (a
 * failure partway through only loses that one rotation, not the whole sweep),
 * same saveAndFlush-before-save ordering to avoid transiently violating
 * idx_edek_current_name (Hibernate's default flush order is by operation type --
 * inserts before updates -- not registration order).
 *
 * <p>Closes a real gap that predates this class: DEKs issued via POST /dek/issue
 * with a name (DbBulkJob's per-column dek-name, FileBulkJob's per-job dek-name)
 * had no rotation at all until now, unlike hsm-core-service's own /encrypt
 * dek_name path.
 */
@Service
public class RotationService {

    private final KekClient kekClient;
    private final EdekRecordRepository edekRecordRepository;
    private final AuditLogger auditLogger;
    private final TransactionTemplate transactionTemplate;

    public RotationService(KekClient kekClient, EdekRecordRepository edekRecordRepository,
                            AuditLogger auditLogger, PlatformTransactionManager transactionManager) {
        this.kekClient = kekClient;
        this.edekRecordRepository = edekRecordRepository;
        this.auditLogger = auditLogger;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public int rotateNamedDeks(int maxAgeHours) {
        OffsetDateTime cutoff = OffsetDateTime.now().minusHours(maxAgeHours);
        List<EdekRecord> candidates = edekRecordRepository.findByRotationStatusAndCurrentDekNameIsNotNullAndCreatedAtBefore(
                RotationStatus.CURRENT, cutoff);

        int rotated = 0;
        for (EdekRecord old : candidates) {
            transactionTemplate.executeWithoutResult(status -> rotateNamedDek(old));
            rotated++;
        }

        auditLogger.log("named_dek_rotation_completed", "records_rotated", rotated, "max_age_hours", maxAgeHours, "status", "success");
        return rotated;
    }

    private void rotateNamedDek(EdekRecord old) {
        byte[] dek = DekManager.generateDek();
        try {
            KekClient.WrapResult wrapResult = kekClient.wrapDek(dek);
            EdekRecord fresh = new EdekRecord(
                    UUID.randomUUID(), old.getAppId(), Base64.getEncoder().encodeToString(wrapResult.edekBytes()), wrapResult.kekVersion(),
                    old.getAlgorithm(), old.getEncoding(), old.getDataClassification(), null, old.getDekName());

            old.setRotationStatus(RotationStatus.ROTATED);
            old.setRotatedAt(OffsetDateTime.now());
            old.clearCurrentDekName();
            // saveAndFlush, not save -- idx_edek_current_name allows only one row per
            // (app_id, current_dek_name); Hibernate's default flush order is by
            // operation type (inserts before updates), not registration order, so
            // fresh's INSERT could otherwise land before old's UPDATE clears its
            // current_dek_name and transiently violate that constraint within the
            // same transaction.
            edekRecordRepository.saveAndFlush(old);
            edekRecordRepository.save(fresh);
        } finally {
            DekManager.zeroDek(dek);
        }
    }
}
