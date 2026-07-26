package com.hsm.core.service;

import com.hsm.core.audit.AuditLogger;
import com.hsm.core.auth.PbacClient;
import com.hsm.core.config.HsmProperties;
import com.hsm.core.crypto.DekManager;
import com.hsm.core.crypto.KekClient;
import com.hsm.core.dto.BatchEncryptItem;
import com.hsm.core.dto.BatchEncryptRequest;
import com.hsm.core.dto.BatchEncryptResponse;
import com.hsm.core.dto.BatchEncryptResultItem;
import com.hsm.core.dto.EncryptRequest;
import com.hsm.core.dto.EncryptResponse;
import com.hsm.core.model.EdekRecord;
import com.hsm.core.repository.EdekRecordRepository;
import com.hsm.core.web.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Ported from app/services/encryption_service.py. */
@Service
public class EncryptionService {

    private static final Logger log = LoggerFactory.getLogger(EncryptionService.class);
    private static final int MAX_PLAINTEXT_BYTES = EncryptRequest.MAX_PLAINTEXT_CHARS;

    private final KekClient kekClient;
    private final EdekRecordRepository edekRecordRepository;
    private final PbacClient pbacClient;
    private final AuditLogger auditLogger;
    private final HsmProperties properties;

    public EncryptionService(KekClient kekClient, EdekRecordRepository edekRecordRepository,
                              PbacClient pbacClient, AuditLogger auditLogger, HsmProperties properties) {
        this.kekClient = kekClient;
        this.edekRecordRepository = edekRecordRepository;
        this.pbacClient = pbacClient;
        this.auditLogger = auditLogger;
        this.properties = properties;
    }

    public EncryptResponse encrypt(EncryptRequest request, String appId, String callerSub, String callerIp) {
        byte[] plaintextBytes = request.plaintext().getBytes(StandardCharsets.UTF_8);
        if (plaintextBytes.length > MAX_PLAINTEXT_BYTES) {
            throw new ApiException(HttpStatus.UNPROCESSABLE_ENTITY,
                    "plaintext exceeds maximum size: " + plaintextBytes.length
                            + " bytes (hard limit " + MAX_PLAINTEXT_BYTES + " bytes)");
        }

        if (request.endUserId() != null && !request.endUserId().isBlank()) {
            boolean permitted = pbacClient.check(
                    request.endUserId(), "encrypt", request.dataClassification(),
                    Map.of("app_id", appId, "caller_ip", nullToEmpty(callerIp))
            );
            if (!permitted) {
                auditLogger.log("encrypt",
                        "app_id", appId, "sub", callerSub, "end_user_id", request.endUserId(),
                        "caller_ip", callerIp, "status", "failure", "reason", "pbac_denied");
                throw new ApiException(HttpStatus.FORBIDDEN, "Access denied by policy");
            }
        }

        byte[] dek = DekManager.generateDek();
        DekManager.EncryptResult result;
        KekClient.WrapResult wrapResult;
        try {
            result = DekManager.encrypt(plaintextBytes, dek, appId);
            wrapResult = kekClient.wrapDek(dek);
        } finally {
            DekManager.zeroDek(dek);
        }

        UUID edekId = UUID.randomUUID();
        String fingerprint = DekManager.makeFingerprint(result.iv(), result.tag());
        EdekRecord record = new EdekRecord(
                edekId, appId, Base64.getEncoder().encodeToString(wrapResult.edekBytes()), wrapResult.kekVersion(),
                DekManager.ALGORITHM, request.encoding(), request.dataClassification(), fingerprint
        );
        edekRecordRepository.save(record);

        auditLogger.log("encrypt",
                "app_id", appId, "sub", callerSub, "end_user_id", request.endUserId(),
                "edek_id", edekId.toString(), "kek_version", wrapResult.kekVersion(),
                "data_classification", request.dataClassification(), "caller_ip", callerIp,
                "context", request.context(), "status", "success");

        return new EncryptResponse(
                DekManager.packToken(edekId, result.iv(), result.tag(), result.ciphertext()),
                edekId, appId, DekManager.ALGORITHM, request.encoding(), wrapResult.kekVersion(),
                Base64.getEncoder().encodeToString(result.iv()),
                Base64.getEncoder().encodeToString(result.ciphertext()),
                Base64.getEncoder().encodeToString(result.tag())
        );
    }

    /**
     * Encrypts every item, reusing {@link #encrypt} unmodified per item --
     * sequential, not concurrent (see java/docs/BULK_OPERATIONS.md: bounded
     * concurrent fan-out is deliberately deferred until real Managed HSM
     * throughput numbers exist; firing items in parallel today risks
     * self-inflicted HSM throttling with no data to size a worker pool by).
     *
     * <p>Structural violations (blank/oversized plaintext, blank/duplicate
     * key, an empty or over-limit batch) reject the whole request via
     * {@link ApiException} before any item is processed -- those are
     * request-shape bugs, not runtime outcomes. A single item's PBAC denial
     * or an unexpected per-item failure does not fail the batch; it's
     * reported in that item's result so every other item still completes.
     */
    public BatchEncryptResponse encryptBatch(BatchEncryptRequest request, String appId, String callerSub, String callerIp) {
        List<BatchEncryptItem> items = request.items();

        int maxItems = properties.service().batchMaxItems();
        if (items.size() > maxItems) {
            throw new ApiException(HttpStatus.UNPROCESSABLE_ENTITY,
                    "batch exceeds maximum item count: " + items.size() + " (limit " + maxItems + ")");
        }

        Set<String> seenKeys = new HashSet<>();
        for (BatchEncryptItem item : items) {
            if (!seenKeys.add(item.key())) {
                throw new ApiException(HttpStatus.UNPROCESSABLE_ENTITY,
                        "duplicate key in batch: '" + item.key() + "' -- each item must have a unique key for correlation");
            }
        }

        List<BatchEncryptResultItem> results = new ArrayList<>(items.size());
        int successCount = 0;
        int failureCount = 0;
        for (BatchEncryptItem item : items) {
            try {
                EncryptRequest single = new EncryptRequest(
                        item.plaintext(), item.encoding(), item.dataClassification(), item.endUserId(), item.context());
                EncryptResponse response = encrypt(single, appId, callerSub, callerIp);
                results.add(BatchEncryptResultItem.success(item.key(), response));
                successCount++;
            } catch (ApiException e) {
                results.add(BatchEncryptResultItem.error(item.key(), e.getMessage()));
                failureCount++;
            } catch (RuntimeException e) {
                log.error("batch_encrypt_item_unexpected_error app_id={} key={} error={}", appId, item.key(), e.getMessage(), e);
                results.add(BatchEncryptResultItem.error(item.key(), "Internal error processing this item"));
                failureCount++;
            }
        }

        auditLogger.log("batch_encrypt",
                "app_id", appId, "sub", callerSub, "item_count", items.size(),
                "success_count", successCount, "failure_count", failureCount, "caller_ip", callerIp, "status", "success");

        return new BatchEncryptResponse(results);
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }
}
