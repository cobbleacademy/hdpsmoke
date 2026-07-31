package com.hsm.bulk.auth;

import java.util.HashMap;
import java.util.Map;

/**
 * Duplicated from com.hsm.core.auth.MockJwtValidator -- SAME fixed demo-token
 * lookup table (demo-token-payments-svc etc.), so a demo-mode bearer token
 * accepted by hsm-core-service is accepted here too. Used when mockKek=true /
 * no real JWT_JWKS_URL is configured, for local PoC runs.
 */
public class MockJwtValidator implements JwtValidator {

    public static final Map<String, Map<String, String>> DEMO_TOKENS = Map.of(
            "demo-token-payments-svc", Map.of("sub", "demo-user-1", "app_id", "payments-svc"),
            "demo-token-reporting-app", Map.of("sub", "demo-user-2", "app_id", "reporting-app"),
            "demo-token-ops-admin", Map.of("sub", "demo-user-3", "app_id", "ops-admin")
    );

    @Override
    public Map<String, Object> validate(String token) throws TokenValidationException {
        Map<String, String> claims = DEMO_TOKENS.get(token);
        if (claims == null) {
            throw new TokenValidationException("Unknown demo token");
        }
        Map<String, Object> result = new HashMap<>();
        result.putAll(claims);
        return result;
    }
}
