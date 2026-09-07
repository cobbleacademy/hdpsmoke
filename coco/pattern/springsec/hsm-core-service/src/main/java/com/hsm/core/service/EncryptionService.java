package com.hsm.core.service;

import com.hsm.core.audit.AuditLogger;
import com.hsm.core.auth.AppRegistryService;
import com.hsm.core.auth.PbacClient;
import com.hsm.core.config.HsmProperties;
import com.hsm.core.crypto.DekCache;
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
import com.hsm.core.web.CorrelationIdFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

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

/** Ported from app/services/encryption_service.py. */
@Service
public class EncryptionService {

    private static final Logger log = LoggerFactory.getLogger(EncryptionService.class);
    private static final int MAX_PLAINTEXT_BYTES = EncryptRequest.MAX_PLAINTEXT_CHARS;

    private final KekClient kekClient;
    private final KekRegistryService kekRegistryService;
    private final EdekRecordRepository edekRecordRepository;
    private final DekCache dekCache;
    private final PbacClient pbacClient;
    private final AuditLogger auditLogger;
    private final HsmProperties properties;
    private final ExecutorService batchExecutor;
    private final AppRegistryService appRegistry;

    public EncryptionService(KekClient kekClient, KekRegistryService kekRegistryService, EdekRecordRepository edekRecordRepository,
                              DekCache dekCache, PbacClient pbacClient, AuditLogger auditLogger, HsmProperties properties,
                              ExecutorService batchExecutor, AppRegistryService appRegistry) {
        this.kekClient = kekClient;
        this.kekRegistryService = kekRegistryService;
        this.edekRecordRepository = edekRecordRepository;
        this.dekCache = dekCache;
        this.pbacClient = pbacClient;
        this.auditLogger = auditLogger;
        this.properties = properties;
        this.batchExecutor = batchExecutor;
        this.appRegistry = appRegistry;
    }

    /**
     * Resolution result feeding encrypt() -- either an existing named DEK (reused=true, unwrapped fresh or
     * from DekCache) or a newly minted one (reused=false, not yet persisted). ownerAppId is the record's
     * permanent owner -- the caller on mint (first-encrypt-wins), or record.getAppId() on reuse, which is
     * NOT necessarily the current caller once V14 cross-app encrypt grants are in play. This must be used
     * as the AES-GCM AAD (and reported as the response's owner_app_id), never the raw caller appId: AAD is
     * fixed forever at the record's true owner because DecryptionService verifies every token against
     * ownerAppId regardless of which grant-authorized app produced it -- using the caller's appId here for
     * a cross-app reuse silently produces a token nothing can ever decrypt again (confirmed by reproducing
     * it end-to-end: owner's grant check passes but tag verification fails; the writer's tag would have
     * matched but its grant check fails, since an encrypt grant doesn't imply a decrypt grant).
     */
    private record ResolvedDek(UUID edekId, byte[] dek, String kekVersion, String kekName, String edekBlobB64, boolean reused, String ownerAppId) {
    }

    public EncryptResponse encrypt(EncryptRequest request, String appId, String callerSub, String callerIp) {
        long requestStart = System.nanoTime();
        log.info("encrypt_request_received app_id={} caller_sub={} caller_ip={} classification={}",
                appId, callerSub, callerIp, request.dataClassification());

        byte[] plaintextBytes = request.plaintext().getBytes(StandardCharsets.UTF_8);
        if (plaintextBytes.length > MAX_PLAINTEXT_BYTES) {
            throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
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

        String dekName = request.dekName();
        long resolveDekStart = System.nanoTime();
        log.info("resolve_dek_started app_id={} dek_name={}", appId, dekName);
        ResolvedDek resolved = resolveDek(appId, dekName, request.dataClassification());
        long resolveDekMs = (System.nanoTime() - resolveDekStart) / 1_000_000;
        log.info("resolve_dek_completed app_id={} dek_name={} reused={} duration_ms={}",
                appId, dekName, resolved.reused(), resolveDekMs);

        DekManager.EncryptResult result;
        try {
            result = DekManager.encrypt(plaintextBytes, resolved.dek(), resolved.ownerAppId());
        } finally {
            DekManager.zeroDek(resolved.dek());
        }

        UUID edekId = resolved.edekId();
        if (!resolved.reused()) {
            // Fingerprint left null for named rows -- a shared DEK legitimately produces
            // a different iv/tag on every call that reuses it, so no single stored
            // fingerprint could validly cross-check every token that will end up
            // referencing this edek_id. DecryptionService's fingerprint check already
            // gates on non-null (see its class comment), so this is a complete fix with
            // zero change needed on the decrypt side.
            boolean named = dekName != null && !dekName.isBlank();
            String fingerprint = named ? null : DekManager.makeFingerprint(result.iv(), result.tag());
            EdekRecord record = new EdekRecord(
                    edekId, appId, resolved.edekBlobB64(), resolved.kekVersion(), resolved.kekName(),
                    DekManager.ALGORITHM, request.encoding(), request.dataClassification(), fingerprint, dekName
            );
            edekRecordRepository.save(record);
        }

        auditLogger.log("encrypt",
                "app_id", appId, "sub", callerSub, "end_user_id", request.endUserId(),
                "edek_id", edekId.toString(), "kek_version", resolved.kekVersion(),
                "data_classification", request.dataClassification(), "caller_ip", callerIp,
                "context", request.context(), "dek_name", dekName, "reused", resolved.reused(), "status", "success");

        long totalMs = (System.nanoTime() - requestStart) / 1_000_000;
        log.info("encrypt_request_completed app_id={} edek_id={} reused={} total_duration_ms={}",
                appId, edekId, resolved.reused(), totalMs);

        return new EncryptResponse(
                DekManager.packToken(edekId, result.iv(), result.tag(), result.ciphertext()),
                edekId, resolved.ownerAppId(), DekManager.ALGORITHM, request.encoding(), resolved.kekVersion(),
                resolved.reused(),
                "success", "ENCRYPT_SUCCESS", "Encryption completed successfully",
                MDC.get(CorrelationIdFilter.MDC_KEY)
        );
    }

    /**
     * dekName unset (or never used before) -&gt; mint fresh, exactly as always. dekName
     * set and already has a "current" row -&gt; reuse that DEK (DekCache hit, or one
     * KEK/HSM unwrap on a miss) instead of minting a new one. First-time-named mints
     * prime DekCache immediately (before the caller zeroes its local copy) so the
     * *next* call for this name is already a cache hit.
     */
    private ResolvedDek resolveDek(String appId, String dekName, String dataClassification) {
        boolean named = dekName != null && !dekName.isBlank();
        if (named) {
            Optional<EdekRecord> existing = edekRecordRepository.findByCurrentDekName(dekName);
            if (existing.isPresent()) {
                EdekRecord record = existing.get();
                // dek_name is globally owned (V14) -- the app that first minted it, not the
                // caller's own scope. A different app may reuse it only with an explicit
                // encrypt grant (coarse: any of the owner's names, or fine-grained: this one
                // specifically); without either, this is a naming collision with someone
                // else's resource, not a free name, and must be rejected outright rather than
                // silently minting a second, unrelated DEK under the same name.
                if (!record.getAppId().equals(appId) && !appRegistry.isGranted(appId, record.getAppId(), "encrypt", dekName)) {
                    throw new ApiException(HttpStatus.FORBIDDEN,
                            "dek_name '" + dekName + "' is owned by app '" + record.getAppId()
                                    + "' -- request an encrypt grant before reusing it");
                }
                checkClassificationMatch(dekName, record.getDataClassification(), dataClassification);
                if ((record.getDataClassification() == null || record.getDataClassification().isBlank())
                        && dataClassification != null && !dataClassification.isBlank()) {
                    // First call never set one; this call did, and there's nothing to
                    // conflict with -- backfill rather than leave it permanently unset.
                    record.setDataClassification(dataClassification);
                    edekRecordRepository.save(record);
                }
                // Self-sufficient: reuse reads the KEK this row was ACTUALLY wrapped
                // under, straight off the record -- never re-resolves via
                // kekRegistryService. That's what makes a later kek_registry change
                // (remapping this dek_name to a different KEK going forward) never
                // retroactively affect already-minted EDEKs -- see EdekRecord's own
                // javadoc. NULL kekName only happens on rows written before this
                // column existed; falls back to the same legacy default
                // KekRegistryService itself uses when nothing is registered at all.
                String kekName = record.getKekName() == null ? kekRegistryService.getLegacyDefaultKekName() : record.getKekName();
                String edekIdStr = record.getEdekId().toString();
                byte[] cached = dekCache.get(edekIdStr);
                byte[] dek;
                if (cached != null) {
                    dek = cached;
                } else {
                    byte[] edekBytes = Base64.getDecoder().decode(record.getEdekBlob());
                    dek = kekClient.unwrapDek(edekBytes, kekName, record.getKekVersion());
                    dekCache.set(edekIdStr, dek, record.getDataClassification());
                }
                return new ResolvedDek(record.getEdekId(), dek, record.getKekVersion(), kekName, null, true, record.getAppId());
            }
        }

        byte[] dek = DekManager.generateDek();
        String kekName = kekRegistryService.resolve(appId, dekName, dataClassification);
        KekClient.WrapResult wrapResult = kekClient.wrapDek(dek, kekName);
        UUID edekId = UUID.randomUUID();
        if (named) {
            dekCache.set(edekId.toString(), dek, dataClassification);
        }
        return new ResolvedDek(edekId, dek, wrapResult.kekVersion(), kekName,
                Base64.getEncoder().encodeToString(wrapResult.edekBytes()), false, appId);
    }

    /** One dek_name is bound to exactly one data_classification -- reject only on an explicit, non-blank conflict; a blank side is a no-op (informational field stays as the other side already has it). */
    private static void checkClassificationMatch(String dekName, String existingClassification, String requestedClassification) {
        if (existingClassification != null && !existingClassification.isBlank()
                && requestedClassification != null && !requestedClassification.isBlank()
                && !existingClassification.equals(requestedClassification)) {
            throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
                    "dek_name '" + dekName + "' is already bound to data_classification '" + existingClassification
                            + "' -- got '" + requestedClassification + "'");
        }
    }

    /**
     * Encrypts every item, reusing {@link #encrypt} unmodified per item.
     * Item-level work is fanned out onto the shared, bounded batchExecutor
     * (hsm.service.batch-executor-pool-size, default 1 -- see
     * BatchExecutorConfig and java/docs/BULK_OPERATIONS.md) rather than run
     * inline; results are collected back in original item order regardless
     * of completion order, since Future#get() is called in submission order.
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
            throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
                    "batch exceeds maximum item count: " + items.size() + " (limit " + maxItems + ")");
        }

        Set<String> seenKeys = new HashSet<>();
        for (BatchEncryptItem item : items) {
            if (!seenKeys.add(item.key())) {
                throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
                        "duplicate key in batch: '" + item.key() + "' -- each item must have a unique key for correlation");
            }
        }

        List<Future<BatchEncryptResultItem>> futures = new ArrayList<>(items.size());
        for (BatchEncryptItem item : items) {
            futures.add(batchExecutor.submit(
                    MdcPropagatingCallable.wrap(() -> encryptBatchItem(item, appId, callerSub, callerIp))));
        }

        List<BatchEncryptResultItem> results = new ArrayList<>(items.size());
        int successCount = 0;
        int failureCount = 0;
        for (Future<BatchEncryptResultItem> future : futures) {
            BatchEncryptResultItem result = awaitBatchItem(future);
            results.add(result);
            if ("success".equals(result.status())) {
                successCount++;
            } else {
                failureCount++;
            }
        }

        auditLogger.log("batch_encrypt",
                "app_id", appId, "sub", callerSub, "item_count", items.size(),
                "success_count", successCount, "failure_count", failureCount, "caller_ip", callerIp, "status", "success");

        return new BatchEncryptResponse(results);
    }

    private BatchEncryptResultItem encryptBatchItem(BatchEncryptItem item, String appId, String callerSub, String callerIp) {
        try {
            EncryptRequest single = new EncryptRequest(
                    item.plaintext(), item.encoding(), item.dataClassification(), item.endUserId(), item.context(), item.dekName());
            EncryptResponse response = encrypt(single, appId, callerSub, callerIp);
            return BatchEncryptResultItem.success(item.key(), response);
        } catch (ApiException e) {
            return BatchEncryptResultItem.error(item.key(), e.getMessage());
        } catch (RuntimeException e) {
            log.error("batch_encrypt_item_unexpected_error app_id={} key={} error={}", appId, item.key(), e.getMessage(), e);
            return BatchEncryptResultItem.error(item.key(), "Internal error processing this item");
        }
    }

    /** encryptBatchItem() catches every RuntimeException itself, so ExecutionException here only ever wraps a JVM-level Error (OOM, etc) -- not a normal per-item failure path. */
    private static BatchEncryptResultItem awaitBatchItem(Future<BatchEncryptResultItem> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "batch processing was interrupted");
        } catch (ExecutionException e) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "batch item processing failed: " + e.getCause());
        }
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }
}
