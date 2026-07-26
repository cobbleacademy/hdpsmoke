package com.hsm.encryption.demo;

import com.hsm.encryption.audit.RecentEventsBuffer;
import com.hsm.encryption.auth.AppRegistryException;
import com.hsm.encryption.auth.AppRegistryService;
import com.hsm.encryption.auth.MockJwtValidator;
import com.hsm.encryption.crypto.KekClient;
import com.hsm.encryption.crypto.MockKekClient;
import com.hsm.encryption.dto.ConsumerAccountCreateRequest;
import com.hsm.encryption.dto.ConsumerAccountResponse;
import com.hsm.encryption.dto.ConsumerRevealRequest;
import com.hsm.encryption.dto.ConsumerRevealResponse;
import com.hsm.encryption.dto.DecryptRequest;
import com.hsm.encryption.dto.DecryptResponse;
import com.hsm.encryption.dto.EncryptRequest;
import com.hsm.encryption.dto.EncryptResponse;
import com.hsm.encryption.model.ConsumerAccount;
import com.hsm.encryption.model.EdekRecord;
import com.hsm.encryption.repository.ConsumerAccountRepository;
import com.hsm.encryption.repository.EdekRecordRepository;
import com.hsm.encryption.service.DecryptionService;
import com.hsm.encryption.service.EncryptionService;
import com.hsm.encryption.web.ApiException;
import jakarta.validation.Valid;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Demo-only endpoints, active only when demo-mode=true. Ported from
 * app/routers/demo.py and app/demo/consumer_store.py.
 */
@RestController
@ConditionalOnProperty(prefix = "hsm", name = "demo-mode", havingValue = "true")
public class DemoController {

    private static final String CONSUMER_OWNER_APP_ID = "payments-svc";

    private final RecentEventsBuffer recentEvents;
    private final KekClient kekClient;
    private final EdekRecordRepository edekRecordRepository;
    private final ConsumerAccountRepository consumerAccountRepository;
    private final AppRegistryService appRegistry;
    private final EncryptionService encryptionService;
    private final DecryptionService decryptionService;

    public DemoController(RecentEventsBuffer recentEvents, KekClient kekClient, EdekRecordRepository edekRecordRepository,
                           ConsumerAccountRepository consumerAccountRepository, AppRegistryService appRegistry,
                           EncryptionService encryptionService, DecryptionService decryptionService) {
        this.recentEvents = recentEvents;
        this.kekClient = kekClient;
        this.edekRecordRepository = edekRecordRepository;
        this.consumerAccountRepository = consumerAccountRepository;
        this.appRegistry = appRegistry;
        this.encryptionService = encryptionService;
        this.decryptionService = decryptionService;
    }

    @GetMapping("${hsm.service.api-v1-prefix}/demo/apps")
    public Map<String, Object> listApps() {
        List<Map<String, Object>> apps = MockJwtValidator.DEMO_TOKENS.entrySet().stream()
                .map(e -> {
                    String token = e.getKey();
                    String appId = e.getValue().get("app_id");
                    Map<String, Object> app = new LinkedHashMap<>();
                    app.put("app_id", appId);
                    app.put("token", token);
                    app.put("scopes", MockJwtValidator.DEMO_SCOPES.getOrDefault(appId, List.of()));
                    return app;
                })
                .toList();
        return Map.of("apps", apps);
    }

    @GetMapping("${hsm.service.api-v1-prefix}/demo/audit-log")
    public Map<String, Object> auditLog(@RequestParam(defaultValue = "50") int limit) {
        return Map.of("events", recentEvents.recent(limit));
    }

    @GetMapping("${hsm.service.api-v1-prefix}/demo/hsm-state")
    public Map<String, Object> hsmState() {
        if (kekClient instanceof MockKekClient mock) {
            MockKekClient.DemoState state = mock.getState();
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("current_version", state.currentVersion());
            result.put("total_versions", state.totalVersions());
            result.put("versions", state.versions());
            return result;
        }
        Map<String, Object> fallback = new LinkedHashMap<>();
        fallback.put("current_version", kekClient.getCurrentKekVersion());
        fallback.put("versions", List.of());
        return fallback;
    }

    @GetMapping("${hsm.service.api-v1-prefix}/demo/edek-records")
    public Map<String, Object> edekRecords(@RequestParam(defaultValue = "20") int limit) {
        List<EdekRecord> records = edekRecordRepository.findAllByOrderByCreatedAtDesc(PageRequest.of(0, limit));
        List<Map<String, Object>> summaries = records.stream().map(r -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("edek_id", r.getEdekId());
            m.put("app_id", r.getAppId());
            m.put("kek_version", r.getKekVersion());
            m.put("algorithm", r.getAlgorithm());
            m.put("encoding", r.getEncoding());
            m.put("data_classification", r.getDataClassification());
            m.put("rotation_status", r.getRotationStatus().name().toLowerCase());
            String blob = r.getEdekBlob();
            m.put("edek_blob_preview", blob.length() > 24 ? blob.substring(0, 24) + "…" : blob);
            m.put("created_at", r.getCreatedAt() != null ? r.getCreatedAt().toString() : null);
            m.put("rotated_at", r.getRotatedAt() != null ? r.getRotatedAt().toString() : null);
            return m;
        }).toList();
        return Map.of("records", summaries);
    }

    @PostMapping("${hsm.service.api-v1-prefix}/demo/consumer/accounts")
    public ResponseEntity<ConsumerAccountResponse> createConsumerAccount(@Valid @RequestBody ConsumerAccountCreateRequest body) {
        // In real life this would be an HTTP call from payments-svc to this service;
        // done in-process here purely to avoid a self-referential network call in the demo.
        EncryptRequest encryptRequest = new EncryptRequest(body.accountNumber(), "utf8", "pci", null, Map.of());
        EncryptResponse enc = encryptionService.encrypt(encryptRequest, CONSUMER_OWNER_APP_ID, "demo-consumer-app", "");
        ConsumerAccount account = new ConsumerAccount(body.customerName(), body.email(), enc.ciphertextToken());
        consumerAccountRepository.save(account);
        return ResponseEntity.status(HttpStatus.CREATED).body(toResponse(account));
    }

    @GetMapping("${hsm.service.api-v1-prefix}/demo/consumer/accounts")
    public Map<String, Object> listConsumerAccounts() {
        List<ConsumerAccountResponse> accounts = consumerAccountRepository.findAllByOrderByCreatedAtDesc().stream()
                .map(this::toResponse).toList();
        return Map.of("accounts", accounts);
    }

    @PostMapping("${hsm.service.api-v1-prefix}/demo/consumer/accounts/{accountId}/reveal")
    public ConsumerRevealResponse revealConsumerAccount(@PathVariable Long accountId, @Valid @RequestBody ConsumerRevealRequest body) {
        ConsumerAccount account = consumerAccountRepository.findById(accountId)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Consumer account not found"));

        List<String> revealScopes;
        try {
            revealScopes = appRegistry.getScopes(body.revealAs());
        } catch (AppRegistryException e) {
            throw new ApiException(HttpStatus.FORBIDDEN, e.getMessage());
        }

        // Relies entirely on decryptionService.decrypt to raise 403 (no grant) or
        // 422 (tag/element mismatch) -- exercises the same grant model as /decrypt,
        // deliberately without an extra "decrypt" scope check here.
        DecryptRequest decryptRequest = new DecryptRequest(account.getCiphertextToken(), null, null, null, null, body.endUserId());
        DecryptResponse dec = decryptionService.decrypt(decryptRequest, body.revealAs(), "demo-consumer-ui", revealScopes, "");
        return new ConsumerRevealResponse(account.getId(), dec.plaintext());
    }

    private ConsumerAccountResponse toResponse(ConsumerAccount account) {
        String createdAt = account.getCreatedAt() != null ? account.getCreatedAt().toString() : null;
        return new ConsumerAccountResponse(account.getId(), account.getCustomerName(), account.getEmail(),
                account.getCiphertextToken(), createdAt);
    }
}
