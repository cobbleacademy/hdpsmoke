package com.hsm.encryption.service;

import com.hsm.encryption.audit.AuditLogger;
import com.hsm.encryption.crypto.KekClient;
import com.hsm.encryption.dto.RotateKekResponse;
import com.hsm.encryption.model.EdekRecord;
import com.hsm.encryption.model.RotationStatus;
import com.hsm.encryption.repository.EdekRecordRepository;
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
