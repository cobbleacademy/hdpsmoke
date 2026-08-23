package com.hsm.core.service;

import com.hsm.core.audit.AuditLogger;
import com.hsm.core.crypto.DekManager;
import com.hsm.core.crypto.KekClient;
import com.hsm.core.dto.RekeyResponse;
import com.hsm.core.dto.RotateKekResponse;
import com.hsm.core.model.EdekRecord;
import com.hsm.core.model.RotationStatus;
import com.hsm.core.repository.EdekRecordRepository;
import com.hsm.core.web.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

/**
 * KEK rotation service. Ported from app/services/rotation_service.py, then
 * extended for multi-KEK support.
 *
 * <p><b>rotateKek</b> (routine, scheduled, automatic): same kek_name, new
 * kek_version. Groups current EDEKs by the distinct kek_name values actually
 * present in edek_records (not kek_registry -- a KEK can be swept here even
 * if kek_registry no longer points any (app_id, dek_name) at it, as long as
 * some already-minted EDEK still uses it), and rewraps each group's
 * lagging records to that KEK's current version. Rows with kek_name = NULL
 * (written before multi-KEK support existed) are swept as part of the
 * legacy-default KEK's group and get their kek_name backfilled in the same
 * pass -- see EdekRecord's javadoc on self-sufficiency.
 *
 * <p><b>rekey</b> (manual, explicit -- compromise response, key
 * decommissioning): moves every current EDEK from one kek_name to a
 * different one. <b>revertRekey</b> (reversion) undoes the last rekey into a
 * given kek_name. Both mutate edek_records in place using the single-level
 * previous_kek_name/previous_kek_version/previous_edek_blob undo buffer on
 * each row (not a multi-row history table, to avoid unbounded growth) and
 * each fire a dedicated AuditLogger event, which is the unbounded historical
 * trail for these operations.
 *
 * <p>Always re-queries page 0 within a group: once a page's records are
 * rewrapped they drop out of that group's filter, so the next unprocessed
 * batch is always at offset 0.
 */
@Service
public class RotationService {

    private static final Logger log = LoggerFactory.getLogger(RotationService.class);
    private static final int PAGE_SIZE = 200;

    private final KekClient kekClient;
    private final KekRegistryService kekRegistryService;
    private final EdekRecordRepository edekRecordRepository;
    private final AuditLogger auditLogger;
    private final TransactionTemplate transactionTemplate;

    public RotationService(KekClient kekClient, KekRegistryService kekRegistryService,
                            EdekRecordRepository edekRecordRepository,
                            AuditLogger auditLogger, PlatformTransactionManager transactionManager) {
        this.kekClient = kekClient;
        this.kekRegistryService = kekRegistryService;
        this.edekRecordRepository = edekRecordRepository;
        this.auditLogger = auditLogger;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public RotateKekResponse rotateKek(String triggeredBy) {
        String legacyDefaultKekName = kekRegistryService.getLegacyDefaultKekName();
        List<String> kekNames = new ArrayList<>(
                edekRecordRepository.findDistinctKekNamesForCurrentRecords(RotationStatus.CURRENT));
        boolean hasLegacyRows = edekRecordRepository.existsByRotationStatusAndKekNameIsNull(RotationStatus.CURRENT);
        if (hasLegacyRows && !kekNames.contains(legacyDefaultKekName)) {
            kekNames.add(legacyDefaultKekName);
        }

        List<RotateKekResponse.KekRotationResult> results = new ArrayList<>(kekNames.size());
        int grandTotal = 0;
        for (String kekName : kekNames) {
            String newVersion = kekClient.getCurrentKekVersion(kekName);
            log.info("kek_rotation_started kek_name={} new_kek_version={} triggered_by={}", kekName, newVersion, triggeredBy);

            int total = rotateGroup(kekName, newVersion, legacyDefaultKekName);

            auditLogger.log("kek_rotation_completed",
                    "kek_name", kekName, "new_kek_version", newVersion, "records_rotated", total,
                    "triggered_by", triggeredBy, "status", "success");
            results.add(new RotateKekResponse.KekRotationResult(kekName, newVersion, total));
            grandTotal += total;
        }

        return new RotateKekResponse(results, grandTotal);
    }

    private int rotateGroup(String kekName, String newVersion, String legacyDefaultKekName) {
        int total = 0;
        while (true) {
            Page<EdekRecord> page = edekRecordRepository.findByRotationStatusAndKekNameAndKekVersionNotOrderByCreatedAtAsc(
                    RotationStatus.CURRENT, kekName, newVersion, PageRequest.of(0, PAGE_SIZE));
            List<EdekRecord> records = page.getContent();
            if (records.isEmpty()) {
                break;
            }
            transactionTemplate.executeWithoutResult(status -> {
                for (EdekRecord record : records) {
                    rewrapRecord(record, kekName, newVersion);
                    edekRecordRepository.save(record);
                }
            });
            total += records.size();
        }

        if (!kekName.equals(legacyDefaultKekName)) {
            return total;
        }
        while (true) {
            Page<EdekRecord> page = edekRecordRepository.findByRotationStatusAndKekNameIsNullAndKekVersionNotOrderByCreatedAtAsc(
                    RotationStatus.CURRENT, newVersion, PageRequest.of(0, PAGE_SIZE));
            List<EdekRecord> records = page.getContent();
            if (records.isEmpty()) {
                break;
            }
            transactionTemplate.executeWithoutResult(status -> {
                for (EdekRecord record : records) {
                    rewrapRecord(record, kekName, newVersion);
                    edekRecordRepository.save(record);
                }
            });
            total += records.size();
        }
        return total;
    }

    private void rewrapRecord(EdekRecord record, String kekName, String newVersion) {
        byte[] oldEdek = Base64.getDecoder().decode(record.getEdekBlob());
        String unwrapKekName = record.getKekName() == null ? kekName : record.getKekName();
        byte[] dekBytes = kekClient.unwrapDek(oldEdek, unwrapKekName, record.getKekVersion());
        try {
            KekClient.WrapResult wrapResult = kekClient.wrapDek(dekBytes, kekName);
            record.setEdekBlob(Base64.getEncoder().encodeToString(wrapResult.edekBytes()));
            record.setKekVersion(newVersion);
            record.setKekName(kekName);
            record.setRotationStatus(RotationStatus.CURRENT);
            record.setRotatedAt(OffsetDateTime.now());
        } finally {
            DekManager.zeroDek(dekBytes);
        }
    }

    /**
     * Moves every current EDEK from fromKekName to toKekName -- manual and
     * explicit, e.g. compromise response or key decommissioning. Unlike
     * rotateKek this changes which key a record is wrapped under, not just
     * that key's version, so it stashes each row's pre-rekey state into its
     * previous_* undo buffer first (see EdekRecord.stashCurrentAsPrevious).
     */
    public RekeyResponse rekey(String fromKekName, String toKekName, String triggeredBy) {
        if (fromKekName.equals(toKekName)) {
            throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT, "fromKekName and toKekName must differ");
        }
        String legacyDefaultKekName = kekRegistryService.getLegacyDefaultKekName();
        String newVersion = kekClient.getCurrentKekVersion(toKekName);

        int total = 0;
        while (true) {
            List<EdekRecord> records = nextGroupPage(fromKekName, legacyDefaultKekName);
            if (records.isEmpty()) {
                break;
            }
            transactionTemplate.executeWithoutResult(status -> {
                for (EdekRecord record : records) {
                    rekeyRecord(record, fromKekName, toKekName, newVersion);
                    edekRecordRepository.save(record);
                }
            });
            total += records.size();
        }

        auditLogger.log("kek_rekey_completed",
                "from_kek_name", fromKekName, "to_kek_name", toKekName, "new_kek_version", newVersion,
                "records_rekeyed", total, "triggered_by", triggeredBy, "status", "success");
        return new RekeyResponse(fromKekName, toKekName, newVersion, total);
    }

    private List<EdekRecord> nextGroupPage(String kekName, String legacyDefaultKekName) {
        List<EdekRecord> named = edekRecordRepository
                .findByRotationStatusAndKekNameOrderByCreatedAtAsc(RotationStatus.CURRENT, kekName, PageRequest.of(0, PAGE_SIZE))
                .getContent();
        if (!named.isEmpty() || !kekName.equals(legacyDefaultKekName)) {
            return named;
        }
        return edekRecordRepository
                .findByRotationStatusAndKekNameIsNullOrderByCreatedAtAsc(RotationStatus.CURRENT, PageRequest.of(0, PAGE_SIZE))
                .getContent();
    }

    private void rekeyRecord(EdekRecord record, String fromKekName, String toKekName, String newVersion) {
        byte[] oldEdek = Base64.getDecoder().decode(record.getEdekBlob());
        byte[] dekBytes = kekClient.unwrapDek(oldEdek, fromKekName, record.getKekVersion());
        try {
            if (record.getKekName() == null) {
                // Backfill so the undo buffer (and any later revertRekey) has a
                // concrete kek_name to restore, instead of reverting to NULL.
                record.setKekName(fromKekName);
            }
            record.stashCurrentAsPrevious();

            KekClient.WrapResult wrapResult = kekClient.wrapDek(dekBytes, toKekName);
            record.setKekName(toKekName);
            record.setKekVersion(newVersion);
            record.setEdekBlob(Base64.getEncoder().encodeToString(wrapResult.edekBytes()));
            record.setRotatedAt(OffsetDateTime.now());
        } finally {
            DekManager.zeroDek(dekBytes);
        }
    }

    /** Undoes the most recent rekey into kekName -- restores each affected row's previous kek_name/kek_version/edek_blob and clears the undo buffer. */
    public RekeyResponse revertRekey(String kekName, String triggeredBy) {
        int total = 0;
        String revertedToKekName = null;
        String revertedToKekVersion = null;
        while (true) {
            Page<EdekRecord> page = edekRecordRepository.findByRotationStatusAndKekNameAndPreviousKekNameIsNotNullOrderByCreatedAtAsc(
                    RotationStatus.CURRENT, kekName, PageRequest.of(0, PAGE_SIZE));
            List<EdekRecord> records = page.getContent();
            if (records.isEmpty()) {
                break;
            }
            for (EdekRecord record : records) {
                revertedToKekName = record.getPreviousKekName();
                revertedToKekVersion = record.getPreviousKekVersion();
            }
            transactionTemplate.executeWithoutResult(status -> {
                for (EdekRecord record : records) {
                    record.restorePreviousAndClear();
                    edekRecordRepository.save(record);
                }
            });
            total += records.size();
        }

        auditLogger.log("kek_rekey_reverted",
                "kek_name", kekName, "reverted_to_kek_name", revertedToKekName,
                "records_reverted", total, "triggered_by", triggeredBy, "status", "success");
        return new RekeyResponse(kekName, revertedToKekName, revertedToKekVersion, total);
    }

    /**
     * Rotates every "current" named DEK (edek_records row with a non-null
     * current_dek_name) whose createdAt is older than maxAgeHours -- one at a time,
     * each in its own transaction, so a failure partway through only loses that one
     * rotation, not the whole sweep. Unlike rotateKek this mints a brand-new DEK per
     * row rather than re-wrapping the existing one -- the DEK material itself is
     * what's being retired, not just its KEK wrapping. Keeps the row's existing
     * kek_name (falling back to the legacy default only for pre-migration rows) --
     * moving a named DEK to a different KEK is what rekey is for, not this.
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
        String kekName = old.getKekName() == null ? kekRegistryService.getLegacyDefaultKekName() : old.getKekName();
        byte[] dek = DekManager.generateDek();
        try {
            KekClient.WrapResult wrapResult = kekClient.wrapDek(dek, kekName);
            EdekRecord fresh = new EdekRecord(
                    UUID.randomUUID(), old.getAppId(), Base64.getEncoder().encodeToString(wrapResult.edekBytes()),
                    wrapResult.kekVersion(), kekName,
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
