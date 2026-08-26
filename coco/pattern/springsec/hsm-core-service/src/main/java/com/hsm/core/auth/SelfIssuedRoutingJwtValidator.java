package com.hsm.core.auth;

import com.hsm.core.config.HsmProperties;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import java.text.ParseException;
import java.util.Map;

/**
 * Routes each incoming bearer token to one of two validators, decided by
 * peeking its (still unverified) {@code iss} claim before anything in the
 * token is trusted -- never a blind try-both chain, which would make
 * ambiguous failures and cross-validator confusion possible.
 *
 * <ul>
 *   <li>Not parseable as a JWT at all -&gt; {@code primary} as-is. In demo
 *       mode this is every MockJwtValidator literal token
 *       ("demo-token-payments-svc" etc., never JWT-shaped); in production
 *       it's whatever malformed input RsaJwtValidator would have rejected
 *       anyway, so the error message stays the one that validator already
 *       produces.</li>
 *   <li>Parses as a JWT whose {@code iss} equals the configured Azure AD
 *       issuer -&gt; {@code primary} (RsaJwtValidator, real production
 *       tokens). In demo mode nothing ever sets this configured issuer to a
 *       value MockJwtValidator's own tokens could match (they aren't JWTs),
 *       so this branch is effectively production-only.</li>
 *   <li>Parses as a JWT with any other {@code iss} -&gt;
 *       {@link SelfSignedAppKeyJwtValidator}, treating {@code iss} as the
 *       caller's claimed app_id pending its own signature verification.</li>
 * </ul>
 */
public class SelfIssuedRoutingJwtValidator implements JwtValidator {

    private final JwtValidator primary;
    private final SelfSignedAppKeyJwtValidator selfSigned;
    private final boolean demoMode;
    private final String configuredIssuer;

    public SelfIssuedRoutingJwtValidator(JwtValidator primary, SelfSignedAppKeyJwtValidator selfSigned, HsmProperties properties) {
        this.primary = primary;
        this.selfSigned = selfSigned;
        this.demoMode = properties.demoMode();
        this.configuredIssuer = properties.jwt().issuer();
    }

    @Override
    public Map<String, Object> validate(String token) throws TokenValidationException {
        SignedJWT parsed;
        try {
            parsed = SignedJWT.parse(token);
        } catch (ParseException e) {
            return primary.validate(token);
        }

        String claimedIssuer;
        try {
            JWTClaimsSet claims = parsed.getJWTClaimsSet();
            claimedIssuer = claims.getIssuer();
        } catch (ParseException e) {
            throw new TokenValidationException("Malformed claims: " + e.getMessage(), e);
        }

        if (!demoMode && claimedIssuer != null && claimedIssuer.equals(configuredIssuer)) {
            return primary.validate(token);
        }
        return selfSigned.validate(token);
    }
}
