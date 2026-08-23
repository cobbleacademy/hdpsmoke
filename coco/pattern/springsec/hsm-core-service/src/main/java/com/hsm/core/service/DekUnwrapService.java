package com.hsm.core.service;

import com.hsm.core.audit.AuditLogger;
import com.hsm.core.auth.AppRegistryException;
import com.hsm.core.auth.AppRegistryService;
import com.hsm.core.config.HsmProperties;
import com.hsm.core.crypto.DekManager;
import com.hsm.core.crypto.KekClient;
import com.hsm.core.crypto.TransportWrapper;
import com.hsm.core.dto.DekUnwrapItem;
import com.hsm.core.dto.DekUnwrapRequest;
import com.hsm.core.dto.DekUnwrapResponse;
import com.hsm.core.dto.DekUnwrapResultItem;
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

/**
 * POST /dek/unwrap -- java/docs/BULK_OPERATIONS.md Tier 3, Phase 2. Per item:
 * resolve the EdekRecord, apply the SAME owner/grant check
 * DecryptionService.decrypt enforces (owner app, an explicit AppDecryptGrant, or
 * the governance authority), unwrap via the real KEK exactly as /decrypt does,
 * then transport-wrap the raw DEK again -- with the REQUESTING app's own public
 * key, not the original owner's -- so only the caller (holding the matching
 * private key) can open it.
 */
@Service
public class DekUnwrapService {

    private static final Logger log = LoggerFactory.getLogger(DekUnwrapService.class);

    private final KekClient kekClient;
    private final KekRegistryService kekRegistryService;
    private final EdekRecordRepository edekRecordRepository;
    private final AppRegistryService appRegistry;
    private final AuditLogger auditLogger;
    private final HsmProperties properties;

    public DekUnwrapService(KekClient kekClient, KekRegistryService kekRegistryService,
                             EdekRecordRepository edekRecordRepository, AppRegistryService appRegistry,
                             AuditLogger auditLogger, HsmProperties properties) {
        this.kekClient = kekClient;
        this.kekRegistryService = kekRegistryService;
        this.edekRecordRepository = edekRecordRepository;
        this.appRegistry = appRegistry;
        this.auditLogger = auditLogger;
        this.properties = properties;
    }

    public DekUnwrapResponse unwrap(DekUnwrapRequest request, String appId, String callerSub,
                                     List<String> callerScopes, String callerIp) {
        List<DekUnwrapItem> items = request.items();

        int maxItems = properties.service().dekBatchMaxItems();
        if (items.size() > maxItems) {
            throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
                    "batch exceeds maximum item count: " + items.size() + " (limit " + maxItems + ")");
        }
        Set<String> seenKeys = new HashSet<>();
        for (DekUnwrapItem item : items) {
            if (!seenKeys.add(item.key())) {
                throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT,
                        "duplicate key in batch: '" + item.key() + "' -- each item must have a unique key for correlation");
            }
        }

        PublicKey callerPublicKey = resolveCallerPublicKey(appId);

        List<DekUnwrapResultItem> results = new ArrayList<>(items.size());
        int successCount = 0;
        int failureCount = 0;
        for (DekUnwrapItem item : items) {
            try {
                results.add(unwrapOne(item, appId, callerScopes, callerPublicKey));
                successCount++;
            } catch (ApiException e) {
                results.add(DekUnwrapResultItem.error(item.key(), e.getMessage()));
                failureCount++;
            } catch (RuntimeException e) {
                log.error("dek_unwrap_item_unexpected_error app_id={} key={} error={}", appId, item.key(), e.getMessage(), e);
                results.add(DekUnwrapResultItem.error(item.key(), "Internal error processing this item"));
                failureCount++;
            }
        }

        auditLogger.log("dek_unwrapped",
                "app_id", appId, "sub", callerSub, "item_count", items.size(),
                "success_count", successCount, "failure_count", failureCount, "caller_ip", callerIp, "status", "success");

        return new DekUnwrapResponse(results);
    }

    private DekUnwrapResultItem unwrapOne(DekUnwrapItem item, String appId, List<String> callerScopes, PublicKey callerPublicKey) {
        Optional<EdekRecord> maybeRecord = edekRecordRepository.findById(item.edekId());
        if (maybeRecord.isEmpty()) {
            throw new ApiException(HttpStatus.NOT_FOUND, "EDEK not found");
        }
        EdekRecord record = maybeRecord.get();
        String ownerAppId = record.getAppId();

        if (!callerScopes.contains("governance") && !appRegistry.isGranted(appId, ownerAppId)) {
            throw new ApiException(HttpStatus.FORBIDDEN, "Access denied");
        }

        String kekName = record.getKekName() == null ? kekRegistryService.getLegacyDefaultKekName() : record.getKekName();
        byte[] edekBytes = Base64.getDecoder().decode(record.getEdekBlob());
        byte[] dek = kekClient.unwrapDek(edekBytes, kekName, record.getKekVersion());
        try {
            byte[] wrappedForTransport = TransportWrapper.wrap(dek, callerPublicKey);
            return DekUnwrapResultItem.success(item.key(), item.edekId(), Base64.getEncoder().encodeToString(wrappedForTransport));
        } finally {
            DekManager.zeroDek(dek);
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
                    "App '" + appId + "' has no public_key_pem registered -- provision one before calling /dek/unwrap "
                            + "(this PoC round has no admin endpoint for this; see BULK_OPERATIONS.md Phase 3)");
        }
        return TransportWrapper.parsePublicKeyPem(pem);
    }
}
