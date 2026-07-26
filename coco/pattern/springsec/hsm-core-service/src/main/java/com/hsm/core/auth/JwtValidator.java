package com.hsm.core.auth;

import java.util.Map;

/**
 * RS256 JWT validation. Tokens must carry sub, app_id (or Azure AD's appid),
 * scope, aud (must match configured audience), iss (must match configured issuer).
 */
public interface JwtValidator {

    /** Decode and validate a Bearer JWT. Returns claims on success. */
    Map<String, Object> validate(String token) throws TokenValidationException;
}
