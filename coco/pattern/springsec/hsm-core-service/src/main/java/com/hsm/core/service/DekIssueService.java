package com.hsm.core.service;

import com.hsm.core.audit.AuditLogger;
import com.hsm.core.auth.AppRegistryException;
import com.hsm.core.auth.AppRegistryService;
import com.hsm.core.config.HsmProperties;
import com.hsm.core.crypto.DekManager;
import com.hsm.core.crypto.KekClient;
import com.hsm.core.crypto.TransportWrapper;
import com.hsm.core.dto.DekIssueItem;
import com.hsm.core.dto.DekIssueRequest;
import com.hsm.core.dto.DekIssueResponse;
import com.hsm.core.dto.DekIssueResultItem;
import com.hsm.core.model.EdekRecord;
import com.hsm.core.repository.EdekRecordRepository;
import com.hsm.core.web.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * POST /dek/issue -- java/docs/BULK_OPERATIONS.md Tier 3, Phase 1. Per item:
 * generate a DEK, KEK-wrap and persist it as a normal EdekRecord exactly like
 * EncryptionService.encrypt does (same fields, same table, same
 * KekRegistryService resolution) -- but skip the AES-GCM step entirely, since
 * the caller (CLNT) does that locally after unwrapping the transport-wrapped
 * DEK this returns. /decrypt can resolve any record created here with no
 * awareness this endpoint exists, since the shared edek_records row shape is
 * identical either way.
 */
@Service
public class DekIssueService {

    private static final Logger log = LoggerFactory.getLogger(DekIssueService.class);

    private final KekClient kekClient;
    private final KekRegistryService kekRegistryService;
    private final EdekRecordRepository edekRecordRepository;
    private final AppRegistryService appRegistry;
    private final AuditLogger auditLogger;
    private final HsmProperties properties;

    public DekIssueService(KekClient kekClient, KekRegistryService kekRegistryService,
                            EdekRecordRepository edekRecordRepository, AppRegistryService appRegistry,
                            AuditLogger auditLogger, HsmProperties properties) {
        this.kekClient = kekClient;
        this.kekRegistryService = kekRegistryService;
        this.edekRecordRepository = edekRecordRepository;
        this.appRegistry = appRegistry;
        this.auditLogger = auditLogger;
        this.properties = properties;
    }

    public DekIssueResponse issue(DekIssueRequest request, String appId, String callerSub, String callerIp) {
        List<DekIssueItem> items = request.items();

        int maxItems = properties.service().dekBatchMaxItems();
        if (items.size() > maxItems) {
            throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
                    "batch exceeds maximum item count: " + items.size() + " (limit " + maxItems + ")");
        }
        Set<String> seenKeys = new HashSet<>();
        for (DekIssueItem item : items) {
            if (!seenKeys.add(item.key())) {
                throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
                        "duplicate key in batch: '" + item.key() + "' -- each item must have a unique key for correlation");
            }
        }

        PublicKey callerPublicKey = resolveCallerPublicKey(appId);

        List<DekIssueResultItem> results = new ArrayList<>(items.size());
        int successCount = 0;
        int failureCount = 0;
        for (DekIssueItem item : items) {
            try {
                results.add(issueOne(item, appId, callerPublicKey));
                successCount++;
            } catch (ApiException e) {
                results.add(DekIssueResultItem.error(item.key(), e.getMessage()));
                failureCount++;
            } catch (RuntimeException e) {
                log.error("dek_issue_item_unexpected_error app_id={} key={} error={}", appId, item.key(), e.getMessage(), e);
                results.add(DekIssueResultItem.error(item.key(), "Internal error processing this item"));
                failureCount++;
            }
        }

        auditLogger.log("dek_issued",
                "app_id", appId, "sub", callerSub, "item_count", items.size(),
                "success_count", successCount, "failure_count", failureCount, "caller_ip", callerIp, "status", "success");

        return new DekIssueResponse(results);
    }

    /**
     * item.name() unset -&gt; mint fresh, exactly as always (DEK-per-item). Set and
     * already has a "current" row for (appId, name) -&gt; reuse it: unwrap the existing
     * edek_blob (one real KEK/HSM operation, same cost as a mint would have been) and
     * transport-wrap that same raw DEK for this caller, rather than minting+persisting
     * a new EdekRecord. No DekCache here (unlike EncryptionService) -- this path was
     * deliberately scoped down without one -- so every reuse still pays one real
     * unwrap; the win is fewer EdekRecord rows / fewer distinct DEKs issued overall,
     * not fewer HSM calls per issuance.
     */
    private DekIssueResultItem issueOne(DekIssueItem item, String appId, PublicKey callerPublicKey) {
        String name = item.name();
        boolean named = name != null && !name.isBlank();

        if (named) {
            Optional<EdekRecord> existing = edekRecordRepository.findByCurrentDekName(name);
            if (existing.isPresent()) {
                EdekRecord record = existing.get();
                // dek_name is globally owned (V14) -- see EncryptionService.resolveDek's
                // identical check for the full reasoning; this is the same gap on the
                // /dek/issue path (hsm-spark-adapter, hsm-bulk-client).
                if (!record.getAppId().equals(appId) && !appRegistry.isGranted(appId, record.getAppId(), "encrypt", name)) {
                    throw new ApiException(HttpStatus.FORBIDDEN,
                            "dek_name '" + name + "' is owned by app '" + record.getAppId()
                                    + "' -- request an encrypt grant before reusing it");
                }
                checkClassificationMatch(name, record.getDataClassification(), item.dataClassification());
                if ((record.getDataClassification() == null || record.getDataClassification().isBlank())
                        && item.dataClassification() != null && !item.dataClassification().isBlank()) {
                    record.setDataClassification(item.dataClassification());
                    edekRecordRepository.save(record);
                }
                // Self-sufficient: reuse reads the KEK this row was actually wrapped
                // under, straight off the record -- see EdekRecord's javadoc.
                String kekName = record.getKekName() == null ? kekRegistryService.getLegacyDefaultKekName() : record.getKekName();
                byte[] edekBytes = Base64.getDecoder().decode(record.getEdekBlob());
                byte[] dek = kekClient.unwrapDek(edekBytes, kekName, record.getKekVersion());
                try {
                    byte[] wrappedForTransport = TransportWrapper.wrap(dek, callerPublicKey);
                    return DekIssueResultItem.success(item.key(), record.getEdekId(),
                            Base64.getEncoder().encodeToString(wrappedForTransport), record.getAppId(), true);
                } finally {
                    DekManager.zeroDek(dek);
                }
            }
        }

        byte[] dek = DekManager.generateDek();
        UUID edekId = UUID.randomUUID();
        try {
            String kekName = kekRegistryService.resolve(appId, name, item.dataClassification());
            KekClient.WrapResult wrapResult = kekClient.wrapDek(dek, kekName);
            EdekRecord record = new EdekRecord(
                    edekId, appId, Base64.getEncoder().encodeToString(wrapResult.edekBytes()), wrapResult.kekVersion(), kekName,
                    DekManager.ALGORITHM, "utf8", item.dataClassification(), null, name);
            edekRecordRepository.save(record);

            byte[] wrappedForTransport = TransportWrapper.wrap(dek, callerPublicKey);
            return DekIssueResultItem.success(item.key(), edekId, Base64.getEncoder().encodeToString(wrappedForTransport), appId, false);
        } finally {
            DekManager.zeroDek(dek);
        }
    }

    /** One name is bound to exactly one data_classification -- reject only on an explicit, non-blank conflict; a blank side is a no-op. Same semantics as EncryptionService.checkClassificationMatch. */
    private static void checkClassificationMatch(String name, String existingClassification, String requestedClassification) {
        if (existingClassification != null && !existingClassification.isBlank()
                && requestedClassification != null && !requestedClassification.isBlank()
                && !existingClassification.equals(requestedClassification)) {
            throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
                    "name '" + name + "' is already bound to data_classification '" + existingClassification
                            + "' -- got '" + requestedClassification + "'");
        }
    }

    private PublicKey resolveCallerPublicKey(String appId) {
        String pem;
        try {
            pem = appRegistry.getPublicKey(appId);
        } catch (AppRegistryException e) {
            throw new ApiException(HttpStatus.FORBIDDEN, e.getMessage());
        }
        if (pem == null) {
            throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
                    "App '" + appId + "' has no public_key_pem registered -- provision one before calling /dek/issue "
                            + "(this PoC round has no admin endpoint for this; see BULK_OPERATIONS.md Phase 3)");
        }
        return TransportWrapper.parsePublicKeyPem(pem);
    }
}
