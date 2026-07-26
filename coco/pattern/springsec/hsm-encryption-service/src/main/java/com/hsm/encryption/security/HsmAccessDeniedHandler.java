package com.hsm.encryption.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsm.encryption.audit.AuditLogger;
import com.hsm.encryption.config.HsmProperties;
import com.hsm.encryption.web.AuthenticatedCaller;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.access.AccessDeniedHandler;
import org.springframework.util.AntPathMatcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Single place scope-denial audit events fire once Spring Security has already
 * blocked an authenticated-but-insufficiently-scoped request before the
 * controller method runs. Necessarily a more generic payload than the old
 * per-controller checks (app_id/sub/path/method/required authorities, not the
 * request-body-specific fields those checks used to include, e.g.
 * grantee_app_id/owner_app_id on a denied grant change) -- see the plan's
 * "Denial handling" note for why that tradeoff was accepted.
 */
public class HsmAccessDeniedHandler implements AccessDeniedHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AntPathMatcher PATH_MATCHER = new AntPathMatcher();

    private final HsmSecurityProperties securityProperties;
    private final String apiV1Prefix;
    private final AuditLogger auditLogger;

    public HsmAccessDeniedHandler(HsmSecurityProperties securityProperties, HsmProperties properties, AuditLogger auditLogger) {
        this.securityProperties = securityProperties;
        this.apiV1Prefix = properties.service().apiV1Prefix();
        this.auditLogger = auditLogger;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response, AccessDeniedException accessDeniedException)
            throws IOException {
        List<String> requiredAuthorities = findRequiredAuthorities(request);
        String requiredLabel = requiredAuthorities.isEmpty() ? "unknown" : requiredAuthorities.get(0);

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        String appId = null;
        String sub = null;
        if (authentication != null && authentication.getPrincipal() instanceof AuthenticatedCaller caller) {
            appId = caller.appId();
            sub = caller.sub();
        }

        auditLogger.log("access_denied",
                "app_id", appId, "sub", sub, "path", request.getRequestURI(), "method", request.getMethod(),
                "required_authorities", requiredAuthorities, "status", "failure",
                "reason", "scope_not_permitted:" + requiredLabel);

        response.setStatus(HttpServletResponse.SC_FORBIDDEN);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getWriter().write(MAPPER.writeValueAsString(Map.of("detail", "Scope '" + requiredLabel + "' not permitted")));
    }

    private List<String> findRequiredAuthorities(HttpServletRequest request) {
        for (HsmSecurityProperties.AccessRule rule : securityProperties.accessRules()) {
            String pattern = apiV1Prefix + rule.pattern();
            if (rule.methods().contains(request.getMethod()) && PATH_MATCHER.match(pattern, request.getRequestURI())) {
                return rule.authorities();
            }
        }
        return List.of();
    }
}
