package com.hsm.core.service;

import com.hsm.core.audit.AuditLogger;
import com.hsm.core.crypto.DekManager;
import com.hsm.core.crypto.KekClient;
import com.hsm.core.dto.RekeyResponse;
import com.hsm.core.dto.RotateKekResponse;
import com.hsm.core.lock.JobAlreadyRunningException;
import com.hsm.core.lock.JobLock;
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
import java.util.function.Supplier;

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
 *
 * <p><b>Single runner.</b> Every pod of every release sharing the database
 * registers the same schedulers, and the admin API can start the same work by
 * hand, so each public entry point runs under a {@link JobLock}. rotateKek,
 * rekey and revertRekey share one lock ({@value #KEK_WRAP_LOCK}) because all
 * three rewrite the same rows' KEK wrapping; rotateNamedDeks has its own. A
 * caller that loses the race gets {@link JobAlreadyRunningException} (409 on
 * the admin API, a logged skip in the schedulers) and a "*_skipped" audit event.
 */
@Service
public class RotationService {

    private static final Logger log = LoggerFactory.getLogger(RotationService.class);
    private static final int PAGE_SIZE = 200;
    static final int MAX_CONSECUTIVE_ROW_FAILURES = 10;

    static final String KEK_WRAP_LOCK = "hsm:kek-wrap";
    static final String NAMED_DEK_ROTATION_LOCK = "hsm:named-dek-rotation";

    private final KekClient kekClient;
    private final KekRegistryService kekRegistryService;
    private final EdekRecordRepository edekRecordRepository;
    private final AuditLogger auditLogger;
    private final TransactionTemplate transactionTemplate;
    private final JobLock jobLock;

    public RotationService(KekClient kekClient, KekRegistryService kekRegistryService,
                            EdekRecordRepository edekRecordRepository,
                            AuditLogger auditLogger, PlatformTransactionManager transactionManager,
                            JobLock jobLock) {
        this.kekClient = kekClient;
        this.kekRegistryService = kekRegistryService;
        this.edekRecordRepository = edekRecordRepository;
        this.auditLogger = auditLogger;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.jobLock = jobLock;
    }

    private <T> T exclusively(String lockKey, String operation, String triggeredBy, Supplier<T> work) {
        return jobLock.tryRun(lockKey, work).orElseThrow(() -> {
            log.info("{}_skipped reason=already_running lock={} triggered_by={}", operation, lockKey, triggeredBy);
            auditLogger.log(operation + "_skipped",
                    "reason", "already_running", "lock", lockKey, "triggered_by", triggeredBy, "status", "skipped");
            return new JobAlreadyRunningException(operation + " skipped: another KEK/DEK rotation job holding lock "
                    + lockKey + " is already running; retry once it completes");
        });
    }

    public RotateKekResponse rotateKek(String triggeredBy) {
        return rotateKek(triggeredBy, null);
    }

    /**
     * onlyKekName null -- sweep every KEK in use (scheduler, and the endpoint
     * with no kekName). Non-null -- sweep just that one KEK's group, e.g. to
     * validate a single Key Vault key rotation in isolation; 404 if no current
     * EDEK is wrapped under it. Passing the legacy-default KEK name still
     * sweeps (and backfills) the kek_name IS NULL rows, same as a full sweep.
     */
    public RotateKekResponse rotateKek(String triggeredBy, String onlyKekName) {
        if (onlyKekName != null && onlyKekName.isBlank()) {
            throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT, "kekName must not be blank");
        }
        return exclusively(KEK_WRAP_LOCK, "kek_rotation", triggeredBy, () -> doRotateKek(triggeredBy, onlyKekName));
    }

    private RotateKekResponse doRotateKek(String triggeredBy, String onlyKekName) {
        String legacyDefaultKekName = kekRegistryService.getLegacyDefaultKekName();
        List<String> kekNames = new ArrayList<>(
                edekRecordRepository.findDistinctKekNamesForCurrentRecords(RotationStatus.CURRENT));
        boolean hasLegacyRows = edekRecordRepository.existsByRotationStatusAndKekNameIsNull(RotationStatus.CURRENT);
        if (hasLegacyRows && !kekNames.contains(legacyDefaultKekName)) {
            kekNames.add(legacyDefaultKekName);
        }
        if (onlyKekName != null) {
            if (!kekNames.contains(onlyKekName)) {
                throw new ApiException(HttpStatus.NOT_FOUND, "No current EDEKs are wrapped under kek_name " + onlyKekName);
            }
            kekNames = List.of(onlyKekName);
        }
        String scope = onlyKekName == null ? "all" : "single";

        List<RotateKekResponse.KekRotationResult> results = new ArrayList<>(kekNames.size());
        int grandTotal = 0;
        for (String kekName : kekNames) {
            String newVersion = kekClient.getCurrentKekVersion(kekName);
            log.info("kek_rotation_started kek_name={} new_kek_version={} triggered_by={}", kekName, newVersion, triggeredBy);

            int total = rotateGroup(kekName, newVersion, legacyDefaultKekName);

            auditLogger.log("kek_rotation_completed",
                    "kek_name", kekName, "new_kek_version", newVersion, "records_rotated", total,
                    "triggered_by", triggeredBy, "scope", scope, "status", "success");
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
        return exclusively(KEK_WRAP_LOCK, "kek_rekey", triggeredBy, () -> doRekey(fromKekName, toKekName, triggeredBy));
    }

    private RekeyResponse doRekey(String fromKekName, String toKekName, String triggeredBy) {
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
        return exclusively(KEK_WRAP_LOCK, "kek_rekey_revert", triggeredBy, () -> doRevertRekey(kekName, triggeredBy));
    }

    private RekeyResponse doRevertRekey(String kekName, String triggeredBy) {
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
     * each in its own transaction. A row that fails (HSM error, constraint
     * violation, ...) is rolled back, logged and audited on its own, and the sweep
     * moves on, so one bad row doesn't leave every row after it unrotated until the
     * next night. Failed rows stay current and are retried on the next run.
     * {@value #MAX_CONSECUTIVE_ROW_FAILURES} failures in a row end the sweep early
     * instead: that pattern means something systemic (Managed HSM or the database is
     * down), and pressing on would only burn HSM calls and flood the logs. Unlike
     * rotateKek this mints a brand-new DEK per row rather than re-wrapping the
     * existing one -- the DEK material itself is what's being retired, not just its
     * KEK wrapping. Keeps the row's existing kek_name (falling back to the legacy
     * default only for pre-migration rows) -- moving a named DEK to a different KEK
     * is what rekey is for, not this.
     */
    public int rotateNamedDeks(int maxAgeHours) {
        return exclusively(NAMED_DEK_ROTATION_LOCK, "named_dek_rotation", "scheduler", () -> doRotateNamedDeks(maxAgeHours));
    }

    private int doRotateNamedDeks(int maxAgeHours) {
        OffsetDateTime cutoff = OffsetDateTime.now().minusHours(maxAgeHours);
        List<EdekRecord> candidates = edekRecordRepository.findByRotationStatusAndCurrentDekNameIsNotNullAndCreatedAtBefore(
                RotationStatus.CURRENT, cutoff);

        int rotated = 0;
        int failed = 0;
        int consecutiveFailures = 0;
        int processed = 0;
        boolean aborted = false;
        for (EdekRecord candidate : candidates) {
            UUID edekId = candidate.getEdekId();
            processed++;
            try {
                if (rotateNamedDekIfStillCurrent(edekId)) {
                    rotated++;
                }
                consecutiveFailures = 0;
            } catch (RuntimeException e) {
                failed++;
                consecutiveFailures++;
                log.error("named_dek_rotation_row_failed edek_id={} error={}", edekId, e.getMessage(), e);
                // Exception type only: messages from the KEK client or JDBC can carry
                // vault URLs or SQL fragments that don't belong in the audit trail.
                auditLogger.log("named_dek_rotation_row_failed",
                        "edek_id", edekId.toString(), "error_type", e.getClass().getSimpleName(), "status", "failure");
                if (consecutiveFailures >= MAX_CONSECUTIVE_ROW_FAILURES) {
                    aborted = true;
                    log.error("named_dek_rotation_aborted consecutive_failures={} rotated={} remaining={}",
                            consecutiveFailures, rotated, candidates.size() - processed);
                    break;
                }
            }
        }

        String status = aborted ? "aborted" : failed > 0 ? "partial_failure" : "success";
        auditLogger.log("named_dek_rotation_completed",
                "records_rotated", rotated, "records_failed", failed, "candidates", candidates.size(),
                "max_age_hours", maxAgeHours, "status", status);
        return rotated;
    }

    /**
     * Re-reads the candidate under a row lock (SELECT ... FOR UPDATE) and rotates it
     * only if it is still the current row for its name. The candidate list is read
     * before any row is touched, so without this a row rotated in the meantime --
     * by anything that bypassed {@link #NAMED_DEK_ROTATION_LOCK} -- would be rotated a
     * second time from a stale copy and fail on idx_edek_current_name.
     */
    boolean rotateNamedDekIfStillCurrent(UUID edekId) {
        Boolean rotated = transactionTemplate.execute(status -> {
            EdekRecord locked = edekRecordRepository.findByIdForUpdate(edekId).orElse(null);
            if (locked == null || locked.getRotationStatus() != RotationStatus.CURRENT || locked.getCurrentDekName() == null) {
                log.info("named_dek_rotation_row_skipped edek_id={} reason=no_longer_current", edekId);
                return false;
            }
            rotateNamedDek(locked);
            return true;
        });
        return Boolean.TRUE.equals(rotated);
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
