package com.hsm.encryption.service;

import com.hsm.encryption.audit.AuditLogger;
import com.hsm.encryption.auth.PbacClient;
import com.hsm.encryption.crypto.DekManager;
import com.hsm.encryption.crypto.KekClient;
import com.hsm.encryption.dto.EncryptRequest;
import com.hsm.encryption.dto.EncryptResponse;
import com.hsm.encryption.model.EdekRecord;
import com.hsm.encryption.repository.EdekRecordRepository;
import com.hsm.encryption.web.ApiException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

/** Ported from app/services/encryption_service.py. */
@Service
public class EncryptionService {

    private static final int MAX_PLAINTEXT_BYTES = EncryptRequest.MAX_PLAINTEXT_CHARS;

    private final KekClient kekClient;
    private final EdekRecordRepository edekRecordRepository;
    private final PbacClient pbacClient;
    private final AuditLogger auditLogger;

    public EncryptionService(KekClient kekClient, EdekRecordRepository edekRecordRepository,
                              PbacClient pbacClient, AuditLogger auditLogger) {
        this.kekClient = kekClient;
        this.edekRecordRepository = edekRecordRepository;
        this.pbacClient = pbacClient;
        this.auditLogger = auditLogger;
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

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }
}
