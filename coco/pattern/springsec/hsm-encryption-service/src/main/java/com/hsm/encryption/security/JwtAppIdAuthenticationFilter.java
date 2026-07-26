package com.hsm.encryption.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsm.encryption.audit.AuditLogger;
import com.hsm.encryption.auth.AppRegistryException;
import com.hsm.encryption.auth.AppRegistryService;
import com.hsm.encryption.auth.JwtValidator;
import com.hsm.encryption.auth.TokenValidationException;
import com.hsm.encryption.web.AuthenticatedCaller;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Bridges the service's existing JWT + X-App-ID authentication model into Spring
 * Security's Authentication/GrantedAuthority. Ported from the logic that used to
 * live in AuthenticatedCallerResolver -- same validation steps, same audit events,
 * same status codes/response bodies on failure; only *where* it runs changed
 * (a filter instead of an argument resolver), so it can run ahead of Spring
 * Security's own authorization decision.
 *
 * <p>Only runs for requests matching one of hsm.security.access-rules (the same
 * externalized config the authorization layer reads) -- everything else (health
 * check, demo endpoints, the static UI) is skipped entirely and needs no token,
 * matching today's behavior.
 */
public class JwtAppIdAuthenticationFilter extends OncePerRequestFilter {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AntPathMatcher PATH_MATCHER = new AntPathMatcher();

    private final JwtValidator jwtValidator;
    private final AppRegistryService appRegistry;
    private final AuditLogger auditLogger;
    private final HsmSecurityProperties securityProperties;
    private final String apiV1Prefix;

    public JwtAppIdAuthenticationFilter(
            JwtValidator jwtValidator,
            AppRegistryService appRegistry,
            AuditLogger auditLogger,
            HsmSecurityProperties securityProperties,
            String apiV1Prefix
    ) {
        this.jwtValidator = jwtValidator;
        this.appRegistry = appRegistry;
        this.auditLogger = auditLogger;
        this.securityProperties = securityProperties;
        this.apiV1Prefix = apiV1Prefix;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        String method = request.getMethod();
        for (HsmSecurityProperties.AccessRule rule : securityProperties.accessRules()) {
            String pattern = apiV1Prefix + rule.pattern();
            if (rule.methods().contains(method) && PATH_MATCHER.match(pattern, path)) {
                return false; // protected -- do not skip
            }
        }
        return true; // not a configured resource -- no auth required
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        String callerIp = request.getRemoteAddr() != null ? request.getRemoteAddr() : "";
        String authorization = request.getHeader("Authorization");
        String xAppId = request.getHeader("X-App-ID");

        if (authorization == null || !authorization.startsWith("Bearer ")) {
            auditLogger.log("auth_failure", "app_id", xAppId, "caller_ip", callerIp,
                    "status", "failure", "reason", "missing_bearer_token");
            writeError(response, HttpServletResponse.SC_UNAUTHORIZED, "Bearer token required");
            return;
        }
        String token = authorization.substring("Bearer ".length());

        Map<String, Object> claims;
        try {
            claims = jwtValidator.validate(token);
        } catch (TokenValidationException e) {
            auditLogger.log("auth_failure", "app_id", xAppId, "caller_ip", callerIp,
                    "status", "failure", "reason", "invalid_token: " + e.getMessage());
            writeError(response, HttpServletResponse.SC_UNAUTHORIZED, e.getMessage());
            return;
        }

        Object tokenAppId = claims.get("app_id");
        if (tokenAppId == null || !tokenAppId.equals(xAppId)) {
            auditLogger.log("auth_failure", "app_id", xAppId, "caller_ip", callerIp,
                    "status", "failure", "reason", "app_id_claim_mismatch", "token_app_id", tokenAppId);
            writeError(response, HttpServletResponse.SC_FORBIDDEN, "app_id claim does not match X-App-ID header");
            return;
        }

        List<String> scopes;
        try {
            scopes = appRegistry.getScopes(xAppId);
        } catch (AppRegistryException e) {
            auditLogger.log("auth_failure", "app_id", xAppId, "caller_ip", callerIp,
                    "status", "failure", "reason", "unknown_or_inactive_app: " + e.getMessage());
            writeError(response, HttpServletResponse.SC_FORBIDDEN, e.getMessage());
            return;
        }

        String sub = claims.get("sub") != null ? String.valueOf(claims.get("sub")) : "";
        AuthenticatedCaller caller = new AuthenticatedCaller(xAppId, sub, new ArrayList<>(scopes));

        List<GrantedAuthority> authorities = new ArrayList<>();
        for (String scope : scopes) {
            authorities.add(new SimpleGrantedAuthority(scope));
        }
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken(caller, null, authorities));

        filterChain.doFilter(request, response);
    }

    private void writeError(HttpServletResponse response, int status, String detail) throws IOException {
        response.setStatus(status);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getWriter().write(MAPPER.writeValueAsString(Map.of("detail", detail)));
    }
}
