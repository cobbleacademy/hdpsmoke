package com.hsm.core.web;

import com.hsm.core.dto.BatchEncryptRequest;
import com.hsm.core.dto.BatchEncryptResponse;
import com.hsm.core.dto.EncryptRequest;
import com.hsm.core.dto.EncryptResponse;
import com.hsm.core.service.EncryptionService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Ported from app/routers/encrypt.py. Scope enforcement ("encrypt" authority)
 * is declarative -- see hsm.security.access-rules (application.yml) and
 * com.hsm.core.security.SecurityConfig; a request without it never
 * reaches this method.
 */
@RestController
public class EncryptController {

    private final EncryptionService encryptionService;

    public EncryptController(EncryptionService encryptionService) {
        this.encryptionService = encryptionService;
    }

    @PostMapping("${hsm.service.api-v1-prefix}/encrypt")
    public ResponseEntity<EncryptResponse> encrypt(
            @Valid @RequestBody EncryptRequest body,
            @AuthenticationPrincipal AuthenticatedCaller caller,
            HttpServletRequest request
    ) {
        String callerIp = request.getRemoteAddr();
        EncryptResponse response = encryptionService.encrypt(body, caller.appId(), caller.sub(), callerIp);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /**
     * Multiple plaintexts in one authenticated call -- see
     * java/docs/BULK_OPERATIONS.md. Each item carries a caller-supplied
     * {@code key} echoed back in its result for correlation; always 200,
     * with a per-item success/error status (mirrors SQS SendMessageBatch)
     * rather than failing the whole batch for one item's runtime outcome.
     */
    @PostMapping("${hsm.service.api-v1-prefix}/encrypt/batch")
    public BatchEncryptResponse encryptBatch(
            @Valid @RequestBody BatchEncryptRequest body,
            @AuthenticationPrincipal AuthenticatedCaller caller,
            HttpServletRequest request
    ) {
        String callerIp = request.getRemoteAddr();
        return encryptionService.encryptBatch(body, caller.appId(), caller.sub(), callerIp);
    }
}
