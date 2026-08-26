package com.hsm.core.auth;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Fake JWT validation for demo mode -- a fixed lookup table, no signature/expiry/
 * issuer verification. Ported from app/demo/mock_jwt_validator.py. Also carries
 * the demo app scope/grant seed data consumed at startup by DemoSeedInitializer.
 */
public class MockJwtValidator implements JwtValidator {

    public static final Map<String, Map<String, String>> DEMO_TOKENS = Map.of(
            "demo-token-payments-svc", Map.of("sub", "demo-user-1", "app_id", "payments-svc"),
            "demo-token-reporting-app", Map.of("sub", "demo-user-2", "app_id", "reporting-app"),
            "demo-token-ops-admin", Map.of("sub", "demo-user-3", "app_id", "ops-admin")
    );

    public static final Map<String, List<String>> DEMO_SCOPES = Map.of(
            "payments-svc", List.of("encrypt", "decrypt", "dek_issue", "dek_unwrap"),
            "reporting-app", List.of("decrypt"),
            "ops-admin", List.of("encrypt", "decrypt", "rotate", "grant", "manage_apps", "provision_app_keys")
    );

    /** (granteeAppId, ownerAppId) pairs. reporting-app may decrypt anything payments-svc encrypted. */
    public static final List<Map.Entry<String, String>> DEMO_GRANTS = List.of(
            Map.entry("reporting-app", "payments-svc")
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
