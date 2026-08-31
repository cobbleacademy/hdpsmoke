package com.hsm.core.auth;

import com.hsm.core.config.HsmProperties;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Verifies a self-issued bearer JWT (RFC 7523-style client authentication) --
 * the caller signs a short-lived assertion locally with its own private key
 * instead of renewing a token from an external IdP, and this validates the
 * signature against that app's registered public key
 * (AppRegistryService.getSigningPublicKey, which falls back to the
 * DEK-transport key when no dedicated signing key is registered -- the
 * legacy one-keypair switch). Built for legacy callers that find OAuth2
 * client-credentials/JWT-renewal machinery operationally painful but can
 * manage a one-time keypair, same operational shape as an SSH key.
 *
 * <p>Deliberately does not consult a JWKS or any external issuer -- the
 * caller's own registered public key IS the trust anchor. See
 * SelfIssuedRoutingJwtValidator for how a request gets routed here instead
 * of to RsaJwtValidator (peeking the unverified {@code iss} claim, before
 * anything in this class is trusted).
 *
 * <p>Replay protection is deliberately NOT built here for this first round --
 * the short TTL (see MAX_TTL) bounds a captured token's reuse window, but
 * nothing tracks {@code jti} to reject an exact replay within that window.
 * Flagged as a known, accepted gap rather than silently absent.
 */
public class SelfSignedAppKeyJwtValidator implements JwtValidator {

    /**
     * Upper bound on exp-iat, enforced here rather than trusted from the
     * token -- the caller fully controls its own claims (unlike an
     * Azure-AD-issued token, where the IdP enforces its own TTL policy), so
     * without a server-side cap a token could claim an arbitrarily long
     * lifetime.
     */
    private static final Duration MAX_TTL = Duration.ofMinutes(5);
    private static final Duration CLOCK_SKEW = Duration.ofSeconds(60);

    private final AppRegistryService appRegistry;
    private final Set<String> acceptedAudiences;

    public SelfSignedAppKeyJwtValidator(AppRegistryService appRegistry, HsmProperties.Jwt jwtConfig) {
        this.appRegistry = appRegistry;
        // hsm.jwt.audience is shared with RsaJwtValidator and accepts the same
        // comma-separated list (e.g. an operator adds an Azure AD v1.0/v2.0 audience
        // pair for AZURE_AD callers) -- parse it the same way here too, or a
        // self-signed caller's single literal aud claim (SelfSignedJwtTokenProvider's
        // "hsm-core-service" default) would never match the whole compound string.
        this.acceptedAudiences = RsaJwtValidator.splitCommaSeparated(jwtConfig.audience());
    }

    @Override
    public Map<String, Object> validate(String token) throws TokenValidationException {
        SignedJWT signedJwt;
        try {
            signedJwt = SignedJWT.parse(token);
        } catch (ParseException e) {
            throw new TokenValidationException("Malformed token header: " + e.getMessage(), e);
        }

        if (!JWSAlgorithm.RS256.equals(signedJwt.getHeader().getAlgorithm())) {
            throw new TokenValidationException("Unsupported JWS algorithm: " + signedJwt.getHeader().getAlgorithm());
        }

        JWTClaimsSet claims;
        try {
            claims = signedJwt.getJWTClaimsSet();
        } catch (ParseException e) {
            throw new TokenValidationException("Malformed claims: " + e.getMessage(), e);
        }

        // Untrusted until the signature below verifies -- this is only which
        // app's registered key to check the signature against.
        String claimedAppId = claims.getSubject();
        if (claimedAppId == null || claimedAppId.isBlank()) {
            throw new TokenValidationException("Missing required claim: sub");
        }

        String signingKeyPem;
        try {
            signingKeyPem = appRegistry.getSigningPublicKey(claimedAppId);
        } catch (AppRegistryException e) {
            throw new TokenValidationException("Unknown or inactive app_id: " + claimedAppId);
        }
        if (signingKeyPem == null) {
            throw new TokenValidationException("No signing or encryption key registered for app_id: " + claimedAppId);
        }
        RSAPublicKey publicKey = parsePemPublicKey(signingKeyPem);

        boolean verified;
        try {
            verified = signedJwt.verify(new RSASSAVerifier(publicKey));
        } catch (Exception e) {
            throw new TokenValidationException("Signature verification failed: " + e.getMessage(), e);
        }
        if (!verified) {
            throw new TokenValidationException("Invalid token signature");
        }

        Instant now = Instant.now();
        Date exp = claims.getExpirationTime();
        Date iat = claims.getIssueTime();
        if (exp == null || iat == null) {
            throw new TokenValidationException("Missing required claim: exp or iat");
        }
        if (exp.toInstant().isBefore(now.minus(CLOCK_SKEW))) {
            throw new TokenValidationException("Token has expired");
        }
        if (iat.toInstant().isAfter(now.plus(CLOCK_SKEW))) {
            throw new TokenValidationException("Token issued in the future");
        }
        if (Duration.between(iat.toInstant(), exp.toInstant()).compareTo(MAX_TTL) > 0) {
            throw new TokenValidationException("Token lifetime exceeds the maximum allowed for a self-issued assertion");
        }

        List<String> audiences = claims.getAudience();
        if (audiences == null || audiences.stream().noneMatch(acceptedAudiences::contains)) {
            throw new TokenValidationException("Invalid audience");
        }

        Map<String, Object> result = new HashMap<>();
        result.put("sub", claimedAppId);
        result.put("app_id", claimedAppId);
        return result;
    }

    private static RSAPublicKey parsePemPublicKey(String pem) throws TokenValidationException {
        try {
            String normalized = pem
                    .replace("-----BEGIN PUBLIC KEY-----", "")
                    .replace("-----END PUBLIC KEY-----", "")
                    .replaceAll("\\s", "");
            byte[] decoded = Base64.getDecoder().decode(normalized);
            X509EncodedKeySpec spec = new X509EncodedKeySpec(decoded);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return (RSAPublicKey) kf.generatePublic(spec);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException | IllegalArgumentException e) {
            throw new TokenValidationException("Invalid registered public key: " + e.getMessage(), e);
        }
    }
}
