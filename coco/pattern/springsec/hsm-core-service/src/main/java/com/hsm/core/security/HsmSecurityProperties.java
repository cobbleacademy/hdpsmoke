package com.hsm.core.security;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Externalized resource-&gt;authority mapping -- the single source of truth for
 * which endpoints require which scope. Read by both JwtAppIdAuthenticationFilter
 * (to decide which requests need a bearer token at all) and SecurityConfig's
 * authorizeHttpRequests (to decide whether an authenticated caller's scopes are
 * sufficient). pattern is relative to hsm.service.api-v1-prefix.
 */
@ConfigurationProperties(prefix = "hsm.security")
public record HsmSecurityProperties(List<AccessRule> accessRules, boolean mtlsEnabled) {

    public record AccessRule(String pattern, List<String> methods, List<String> authorities) {
    }
}
