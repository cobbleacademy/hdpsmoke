package com.hsm.encryption.web;

import com.hsm.encryption.dto.EncryptRequest;
import com.hsm.encryption.dto.EncryptResponse;
import com.hsm.encryption.service.EncryptionService;
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
 * com.hsm.encryption.security.SecurityConfig; a request without it never
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
}
