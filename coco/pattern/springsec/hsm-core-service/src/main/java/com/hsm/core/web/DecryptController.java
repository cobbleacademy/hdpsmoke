package com.hsm.core.web;

import com.hsm.core.dto.BatchDecryptRequest;
import com.hsm.core.dto.BatchDecryptResponse;
import com.hsm.core.dto.DecryptRequest;
import com.hsm.core.dto.DecryptResponse;
import com.hsm.core.service.DecryptionService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Ported from app/routers/decrypt.py. Scope enforcement ("decrypt" authority)
 * is declarative -- see hsm.security.access-rules (application.yml) and
 * com.hsm.core.security.SecurityConfig; a request without it never
 * reaches this method. The "governance" bypass-grant-check scope stays a
 * business-logic decision inside DecryptionService itself, not a URL rule.
 */
@RestController
public class DecryptController {

    private final DecryptionService decryptionService;

    public DecryptController(DecryptionService decryptionService) {
        this.decryptionService = decryptionService;
    }

    @PostMapping("${hsm.service.api-v1-prefix}/decrypt")
    public DecryptResponse decrypt(
            @RequestBody DecryptRequest body,
            @AuthenticationPrincipal AuthenticatedCaller caller,
            HttpServletRequest request
    ) {
        String callerIp = ClientIpResolver.resolve(request);
        return decryptionService.decrypt(body, caller.appId(), caller.sub(), caller.scopes(), callerIp);
    }

    /**
     * Multiple ciphertexts in one authenticated call -- see
     * java/docs/BULK_OPERATIONS.md. Same key-correlation and always-200
     * partial-failure semantics as POST /encrypt/batch.
     */
    @PostMapping("${hsm.service.api-v1-prefix}/decrypt/batch")
    public BatchDecryptResponse decryptBatch(
            @Valid @RequestBody BatchDecryptRequest body,
            @AuthenticationPrincipal AuthenticatedCaller caller,
            HttpServletRequest request
    ) {
        String callerIp = ClientIpResolver.resolve(request);
        return decryptionService.decryptBatch(body, caller.appId(), caller.sub(), caller.scopes(), callerIp);
    }
}
