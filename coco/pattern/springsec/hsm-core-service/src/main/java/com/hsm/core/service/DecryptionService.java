package com.hsm.core.service;

import com.hsm.core.audit.AuditLogger;
import com.hsm.core.auth.AppRegistryService;
import com.hsm.core.auth.PbacClient;
import com.hsm.core.config.HsmProperties;
import com.hsm.core.crypto.DekCache;
import com.hsm.core.crypto.DekManager;
import com.hsm.core.crypto.KekClient;
import com.hsm.core.dto.BatchDecryptItem;
import com.hsm.core.dto.BatchDecryptRequest;
import com.hsm.core.dto.BatchDecryptResponse;
import com.hsm.core.dto.BatchDecryptResultItem;
import com.hsm.core.dto.DecryptRequest;
import com.hsm.core.dto.DecryptResponse;
import com.hsm.core.model.EdekRecord;
import com.hsm.core.repository.EdekRecordRepository;
import com.hsm.core.web.ApiException;
import com.hsm.core.web.CorrelationIdFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import javax.crypto.AEADBadTagException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/** Ported from app/services/decryption_service.py. */
@Service
public class DecryptionService {

    private static final Logger log = LoggerFactory.getLogger(DecryptionService.class);

    private final KekClient kekClient;
    private final KekRegistryService kekRegistryService;
    private final EdekRecordRepository edekRecordRepository;
    private final AppRegistryService appRegistry;
    private final DekCache dekCache;
    private final PbacClient pbacClient;
    private final AuditLogger auditLogger;
    private final HsmProperties properties;
    private final ExecutorService batchExecutor;

    public DecryptionService(KekClient kekClient, KekRegistryService kekRegistryService,
                              EdekRecordRepository edekRecordRepository,
                              AppRegistryService appRegistry, DekCache dekCache,
                              PbacClient pbacClient, AuditLogger auditLogger, HsmProperties properties,
                              ExecutorService batchExecutor) {
        this.kekClient = kekClient;
        this.kekRegistryService = kekRegistryService;
        this.edekRecordRepository = edekRecordRepository;
        this.appRegistry = appRegistry;
        this.dekCache = dekCache;
        this.pbacClient = pbacClient;
        this.auditLogger = auditLogger;
        this.properties = properties;
        this.batchExecutor = batchExecutor;
    }

    public DecryptResponse decrypt(DecryptRequest request, String appId, String callerSub, List<String> callerScopes, String callerIp) {
        // -- Resolve inputs: token path (preferred) or legacy individual fields --
        UUID resolvedEdekId;
        byte[] resolvedIv;
        byte[] resolvedTag;
        byte[] resolvedCiphertext;

        if (request.ciphertext() != null) {
            DekManager.UnpackedToken unpacked;
            try {
                unpacked = DekManager.unpackToken(request.ciphertext());
            } catch (IllegalArgumentException e) {
                throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT, e.getMessage());
            }
            resolvedEdekId = unpacked.edekId();
            resolvedIv = unpacked.iv();
            resolvedTag = unpacked.tag();
            resolvedCiphertext = unpacked.ciphertext();
        } else {
            if (request.edekId() == null) {
                throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
                        "Provide either 'ciphertext' (recommended) or the legacy fields "
                                + "'edek_id', 'iv_b64', 'ciphertext_b64', 'tag_b64'");
            }
            if (request.ivB64() == null || request.ciphertextB64() == null || request.tagB64() == null) {
                throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
                        "Legacy decrypt is missing required fields. Use 'ciphertext' instead to avoid this.");
            }
            resolvedEdekId = request.edekId();
            try {
                resolvedIv = Base64.getDecoder().decode(request.ivB64());
                resolvedCiphertext = Base64.getDecoder().decode(request.ciphertextB64());
                resolvedTag = Base64.getDecoder().decode(request.tagB64());
            } catch (IllegalArgumentException e) {
                throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT, "field must be valid base64");
            }
        }

        String edekIdStr = resolvedEdekId.toString();
        Optional<EdekRecord> maybeRecord = edekRecordRepository.findById(resolvedEdekId);
        if (maybeRecord.isEmpty()) {
            auditFail(appId, callerSub, edekIdStr, callerIp, "edek_not_found", request.endUserId(), null);
            throw new ApiException(HttpStatus.NOT_FOUND, "EDEK not found");
        }
        EdekRecord record = maybeRecord.get();
        String ownerAppId = record.getAppId();

        // Governance SPN bypasses the per-record grant check -- it may decrypt any
        // record for audit purposes. All other callers must have an explicit grant.
        if (!callerScopes.contains("governance")) {
            if (!appRegistry.isGranted(appId, ownerAppId)) {
                auditFail(appId, callerSub, edekIdStr, callerIp, "no_grant_for_owner", request.endUserId(), ownerAppId);
                throw new ApiException(HttpStatus.FORBIDDEN, "Access denied");
            }
        }

        if (request.endUserId() != null && !request.endUserId().isBlank()) {
            boolean permitted = pbacClient.check(
                    request.endUserId(), "decrypt", record.getDataClassification(),
                    Map.of("app_id", appId, "owner_app_id", ownerAppId, "caller_ip", nullToEmpty(callerIp))
            );
            if (!permitted) {
                auditFail(appId, callerSub, edekIdStr, callerIp, "pbac_denied", request.endUserId(), null);
                throw new ApiException(HttpStatus.FORBIDDEN, "Access denied by policy");
            }
        }

        // -- Pre-flight: fixed-size parameter checks (legacy path only) --
        // Token path: iv/tag sizes are guaranteed by pack_token -- no check needed.
        if (request.ciphertext() == null) {
            if (resolvedIv.length != DekManager.IV_LENGTH) {
                auditFail(appId, callerSub, edekIdStr, callerIp, "invalid_iv_length", request.endUserId(), null);
                throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
                        "iv_b64 is invalid: decoded to " + resolvedIv.length + " bytes, AES-GCM requires exactly "
                                + DekManager.IV_LENGTH + " bytes (96-bit nonce)");
            }
            if (resolvedTag.length != DekManager.TAG_LENGTH) {
                auditFail(appId, callerSub, edekIdStr, callerIp, "invalid_tag_length", request.endUserId(), null);
                throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
                        "tag_b64 is invalid: decoded to " + resolvedTag.length + " bytes, AES-GCM requires exactly "
                                + DekManager.TAG_LENGTH + " bytes (128-bit tag)");
            }
        }

        // -- Pre-flight: fingerprint cross-check --
        // Only runs when the record has a fingerprint (pre-existing records skip this).
        if (record.getFingerprint() != null) {
            String expected = DekManager.makeFingerprint(resolvedIv, resolvedTag);
            if (!expected.equals(record.getFingerprint())) {
                auditFail(appId, callerSub, edekIdStr, callerIp, "element_mismatch", request.endUserId(), null);
                throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
                        "iv_b64, ciphertext_b64, or tag_b64 do not belong to this edek_id. These fields must all "
                                + "come from the same encrypt response -- mixing elements across different responses is not permitted.");
            }
        }

        byte[] edekBytes = Base64.getDecoder().decode(record.getEdekBlob());

        byte[] cachedDek = dekCache.get(edekIdStr);
        byte[] dek;
        if (cachedDek != null) {
            dek = cachedDek;
        } else {
            String kekName = record.getKekName() == null ? kekRegistryService.getLegacyDefaultKekName() : record.getKekName();
            dek = kekClient.unwrapDek(edekBytes, kekName, record.getKekVersion());
            dekCache.set(edekIdStr, dek, record.getDataClassification());
        }

        byte[] plaintext;
        try {
            plaintext = DekManager.decrypt(resolvedCiphertext, resolvedTag, resolvedIv, dek, ownerAppId);
        } catch (AEADBadTagException e) {
            auditFail(appId, callerSub, edekIdStr, callerIp, "tag_verification_failed", request.endUserId(), null);
            throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
                    "Ciphertext authentication failed: the data may be corrupt or tampered with");
        } finally {
            DekManager.zeroDek(dek);
        }

        auditLogger.log("decrypt",
                "app_id", appId, "owner_app_id", ownerAppId, "sub", callerSub, "end_user_id", request.endUserId(),
                "edek_id", edekIdStr, "kek_version", record.getKekVersion(), "caller_ip", callerIp, "status", "success");

        return new DecryptResponse(new String(plaintext, StandardCharsets.UTF_8), ownerAppId, record.getAlgorithm(), record.getEncoding(),
                "success", "DECRYPT_SUCCESS", "Decryption completed successfully",
                MDC.get(CorrelationIdFilter.MDC_KEY));
    }

    /**
     * Decrypts every item, reusing {@link #decrypt} unmodified per item.
     * Item-level work is fanned out onto the shared, bounded batchExecutor
     * (hsm.service.batch-executor-pool-size -- see BatchExecutorConfig and
     * java/docs/BULK_OPERATIONS.md), same pattern as
     * EncryptionService.encryptBatch; results are collected back in
     * original item order. Unlike batch encrypt, the "token or legacy
     * fields" either-or check is not expressible as a static Bean
     * Validation constraint -- it lives inside decrypt() itself -- so a
     * malformed item there naturally becomes that item's error rather than
     * a whole-batch rejection, on top of the same rejection for a truly
     * structural violation (empty/over-cap batch, duplicate key).
     */
    public BatchDecryptResponse decryptBatch(BatchDecryptRequest request, String appId, String callerSub,
                                              List<String> callerScopes, String callerIp) {
        List<BatchDecryptItem> items = request.items();

        int maxItems = properties.service().batchMaxItems();
        if (items.size() > maxItems) {
            throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
                    "batch exceeds maximum item count: " + items.size() + " (limit " + maxItems + ")");
        }

        Set<String> seenKeys = new HashSet<>();
        for (BatchDecryptItem item : items) {
            if (!seenKeys.add(item.key())) {
                throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
                        "duplicate key in batch: '" + item.key() + "' -- each item must have a unique key for correlation");
            }
        }

        List<Future<BatchDecryptResultItem>> futures = new ArrayList<>(items.size());
        for (BatchDecryptItem item : items) {
            futures.add(batchExecutor.submit(
                    MdcPropagatingCallable.wrap(() -> decryptBatchItem(item, appId, callerSub, callerScopes, callerIp))));
        }

        List<BatchDecryptResultItem> results = new ArrayList<>(items.size());
        int successCount = 0;
        int failureCount = 0;
        for (Future<BatchDecryptResultItem> future : futures) {
            BatchDecryptResultItem result = awaitBatchItem(future);
            results.add(result);
            if ("success".equals(result.status())) {
                successCount++;
            } else {
                failureCount++;
            }
        }

        auditLogger.log("batch_decrypt",
                "app_id", appId, "sub", callerSub, "item_count", items.size(),
                "success_count", successCount, "failure_count", failureCount, "caller_ip", callerIp, "status", "success");

        return new BatchDecryptResponse(results);
    }

    private BatchDecryptResultItem decryptBatchItem(BatchDecryptItem item, String appId, String callerSub,
                                                      List<String> callerScopes, String callerIp) {
        try {
            DecryptRequest single = new DecryptRequest(
                    item.ciphertext(), item.edekId(), item.ivB64(), item.ciphertextB64(), item.tagB64(), item.endUserId());
            DecryptResponse response = decrypt(single, appId, callerSub, callerScopes, callerIp);
            return BatchDecryptResultItem.success(item.key(), response);
        } catch (ApiException e) {
            return BatchDecryptResultItem.error(item.key(), e.getMessage());
        } catch (RuntimeException e) {
            log.error("batch_decrypt_item_unexpected_error app_id={} key={} error={}", appId, item.key(), e.getMessage(), e);
            return BatchDecryptResultItem.error(item.key(), "Internal error processing this item");
        }
    }

    /** decryptBatchItem() catches every RuntimeException itself, so ExecutionException here only ever wraps a JVM-level Error (OOM, etc) -- not a normal per-item failure path. */
    private static BatchDecryptResultItem awaitBatchItem(Future<BatchDecryptResultItem> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "batch processing was interrupted");
        } catch (ExecutionException e) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "batch item processing failed: " + e.getCause());
        }
    }

    private void auditFail(String appId, String sub, String edekId, String ip, String reason, String endUserId, String ownerAppId) {
        if (ownerAppId != null) {
            auditLogger.log("decrypt", "app_id", appId, "sub", sub, "edek_id", edekId, "caller_ip", ip,
                    "status", "failure", "reason", reason, "owner_app_id", ownerAppId, "end_user_id", endUserId);
        } else {
            auditLogger.log("decrypt", "app_id", appId, "sub", sub, "edek_id", edekId, "caller_ip", ip,
                    "status", "failure", "reason", reason, "end_user_id", endUserId);
        }
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }
}
