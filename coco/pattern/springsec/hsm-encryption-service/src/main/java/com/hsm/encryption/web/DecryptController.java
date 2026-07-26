package com.hsm.encryption.web;

import com.hsm.encryption.dto.DecryptRequest;
import com.hsm.encryption.dto.DecryptResponse;
import com.hsm.encryption.service.DecryptionService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Ported from app/routers/decrypt.py. Scope enforcement ("decrypt" authority)
 * is declarative -- see hsm.security.access-rules (application.yml) and
 * com.hsm.encryption.security.SecurityConfig; a request without it never
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
        String callerIp = request.getRemoteAddr();
        return decryptionService.decrypt(body, caller.appId(), caller.sub(), caller.scopes(), callerIp);
    }
}
