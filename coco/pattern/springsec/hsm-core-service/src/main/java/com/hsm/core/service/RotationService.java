package com.hsm.core.service;

import com.hsm.core.audit.AuditLogger;
import com.hsm.core.crypto.DekManager;
import com.hsm.core.crypto.KekClient;
import com.hsm.core.dto.RotateKekResponse;
import com.hsm.core.model.EdekRecord;
import com.hsm.core.model.RotationStatus;
import com.hsm.core.repository.EdekRecordRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

/**
 * KEK rotation service. Ported from app/services/rotation_service.py.
 *
 * <p>Strategy: page through all EDEK records with status=current whose kek_version
 * differs from the target version; for each, unwrap with the old version, re-wrap
 * with the new one, and update the record; commit per page. The old KEK version
 * stays usable in Key Vault (disabled, not deleted) so in-flight decrypts during
 * the rotation window still work.
 *
 * <p>Always re-queries page 0: once a page's records are re-wrapped their
 * kek_version equals the target, so they naturally drop out of the "current &amp;
 * kek_version != target" filter -- the next unprocessed batch is always at
 * offset 0. (The Python source instead increments a page offset across a
 * shrinking filtered result set, which would skip records; this port fixes that.)
 */
@Service
public class RotationService {

    private static final Logger log = LoggerFactory.getLogger(RotationService.class);
    private static final int PAGE_SIZE = 200;

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

    public RotateKekResponse rotateKek(String triggeredBy) {
        String newVersion = kekClient.getCurrentKekVersion();
        log.info("kek_rotation_started new_kek_version={} triggered_by={}", newVersion, triggeredBy);

        int total = 0;
        while (true) {
            Page<EdekRecord> page = edekRecordRepository.findByRotationStatusAndKekVersionNotOrderByCreatedAtAsc(
                    RotationStatus.CURRENT, newVersion, PageRequest.of(0, PAGE_SIZE));
            List<EdekRecord> records = page.getContent();
            if (records.isEmpty()) {
                break;
            }
            transactionTemplate.executeWithoutResult(status -> {
                for (EdekRecord record : records) {
                    rewrapRecord(record, newVersion);
                    edekRecordRepository.save(record);
                }
            });
            total += records.size();
        }

        auditLogger.log("kek_rotation_completed",
                "new_kek_version", newVersion, "records_rotated", total, "triggered_by", triggeredBy, "status", "success");

        return new RotateKekResponse(newVersion, total);
    }

    /**
     * Rotates every "current" named DEK (edek_records row with a non-null
     * current_dek_name) whose createdAt is older than maxAgeHours -- one at a time,
     * each in its own transaction, so a failure partway through only loses that one
     * rotation, not the whole sweep. Unlike rotateKek this mints a brand-new DEK per
     * row rather than re-wrapping the existing one -- the DEK material itself is
     * what's being retired, not just its KEK wrapping.
     */
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

    private void rewrapRecord(EdekRecord record, String newVersion) {
        byte[] oldEdek = Base64.getDecoder().decode(record.getEdekBlob());
        byte[] dekBytes = kekClient.unwrapDek(oldEdek, record.getKekVersion());
        KekClient.WrapResult wrapResult = kekClient.wrapDek(dekBytes);

        record.setEdekBlob(Base64.getEncoder().encodeToString(wrapResult.edekBytes()));
        record.setKekVersion(newVersion);
        record.setRotationStatus(RotationStatus.CURRENT);
        record.setRotatedAt(OffsetDateTime.now());
    }
}
