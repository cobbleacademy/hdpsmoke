package com.hsm.bulk.auth;

import java.util.Map;

/** Duplicated from com.hsm.core.auth.JwtValidator -- validates the SAME per-app JWTs hsm-core-service does (shared JWKS/issuer/audience config). */
public interface JwtValidator {

    Map<String, Object> validate(String token) throws TokenValidationException;
}
