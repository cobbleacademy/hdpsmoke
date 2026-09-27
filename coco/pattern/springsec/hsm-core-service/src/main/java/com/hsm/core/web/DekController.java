package com.hsm.core.web;

import com.hsm.core.dto.DekIssueRequest;
import com.hsm.core.dto.DekIssueResponse;
import com.hsm.core.dto.DekUnwrapRequest;
import com.hsm.core.dto.DekUnwrapResponse;
import com.hsm.core.service.DekIssueService;
import com.hsm.core.service.DekUnwrapService;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Scope enforcement (dek_issue / dek_unwrap authorities) is declarative -- see
 * hsm.security.access-rules (application.yml) and com.hsm.core.security.SecurityConfig;
 * a request without the right authority never reaches this method, same pattern
 * as EncryptController/DecryptController.
 */
@RestController
@Tag(name = "DEK (Tier 3)", description = "Issue or unwrap DEKs, RSA-OAEP-wrapped to the caller's registered public key, for local encryption (hsm-crypto-client, hsm-bulk-client, hsm-file-service).")
public class DekController {

    private final DekIssueService dekIssueService;
    private final DekUnwrapService dekUnwrapService;

    public DekController(DekIssueService dekIssueService, DekUnwrapService dekUnwrapService) {
        this.dekIssueService = dekIssueService;
        this.dekUnwrapService = dekUnwrapService;
    }

    @PostMapping("${hsm.service.api-v1-prefix}/dek/issue")
    public DekIssueResponse issue(
            @Valid @RequestBody DekIssueRequest body,
            @AuthenticationPrincipal AuthenticatedCaller caller,
            HttpServletRequest request
    ) {
        String callerIp = request.getRemoteAddr();
        return dekIssueService.issue(body, caller.appId(), caller.sub(), callerIp);
    }

    @PostMapping("${hsm.service.api-v1-prefix}/dek/unwrap")
    public DekUnwrapResponse unwrap(
            @Valid @RequestBody DekUnwrapRequest body,
            @AuthenticationPrincipal AuthenticatedCaller caller,
            HttpServletRequest request
    ) {
        String callerIp = request.getRemoteAddr();
        return dekUnwrapService.unwrap(body, caller.appId(), caller.sub(), caller.scopes(), callerIp);
    }
}
