package com.hsm.encryption.auth;

import com.hsm.encryption.config.HsmProperties;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
import java.util.concurrent.locks.ReentrantLock;

/** Ported from app/auth/jwt_validator.py's JWTValidator, backed by Nimbus JOSE + JWT. */
public class RsaJwtValidator implements JwtValidator {

    private static final long JWKS_TTL_SECONDS = 3600; // re-fetch JWKS every hour

    private final HsmProperties.Jwt config;
    private final HttpClient httpClient;
    private final ReentrantLock lock = new ReentrantLock();

    private volatile JWKSet cachedJwks;
    private volatile long jwksFetchedAtEpochSeconds = 0;

    public RsaJwtValidator(HsmProperties.Jwt config) {
        this.config = config;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
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

        RSAPublicKey publicKey = resolveKey(signedJwt.getHeader().getKeyID());

        boolean verified;
        try {
            verified = signedJwt.verify(new RSASSAVerifier(publicKey));
        } catch (Exception e) {
            throw new TokenValidationException("Signature verification failed: " + e.getMessage(), e);
        }
        if (!verified) {
            throw new TokenValidationException("Invalid token signature");
        }

        JWTClaimsSet claims;
        try {
            claims = signedJwt.getJWTClaimsSet();
        } catch (ParseException e) {
            throw new TokenValidationException("Malformed claims: " + e.getMessage(), e);
        }

        Instant now = Instant.now();
        Date exp = claims.getExpirationTime();
        if (exp != null && exp.toInstant().isBefore(now)) {
            throw new TokenValidationException("Token has expired");
        }
        Date nbf = claims.getNotBeforeTime();
        if (nbf != null && nbf.toInstant().isAfter(now)) {
            throw new TokenValidationException("Token not yet valid");
        }

        List<String> audiences = claims.getAudience();
        if (audiences == null || !audiences.contains(config.audience())) {
            throw new TokenValidationException("Invalid audience");
        }
        String issuer = claims.getIssuer();
        if (issuer == null || !issuer.equals(config.issuer())) {
            throw new TokenValidationException("Invalid issuer");
        }

        Map<String, Object> result = new HashMap<>(claims.getClaims());
        // Accept Azure AD's built-in "appid" claim as equivalent to "app_id"
        if (!result.containsKey("app_id")) {
            if (result.containsKey("appid")) {
                result.put("app_id", result.get("appid"));
            } else {
                throw new TokenValidationException("Missing required claim: app_id or appid");
            }
        }
        return result;
    }

    private RSAPublicKey resolveKey(String kid) throws TokenValidationException {
        if (!config.publicKeyPem().isBlank()) {
            return parsePemPublicKey(config.publicKeyPem());
        }

        JWKSet jwks = getJwks();
        JWK jwk = kid != null ? jwks.getKeyByKeyId(kid) : null;
        if (jwk == null) {
            throw new TokenValidationException("No matching key for kid=" + kid);
        }
        try {
            return ((RSAKey) jwk).toRSAPublicKey();
        } catch (Exception e) {
            throw new TokenValidationException("Failed to construct RSA public key from JWKS: " + e.getMessage(), e);
        }
    }

    private JWKSet getJwks() throws TokenValidationException {
        long now = Instant.now().getEpochSecond();
        JWKSet snapshot = cachedJwks;
        if (snapshot != null && (now - jwksFetchedAtEpochSeconds) < JWKS_TTL_SECONDS) {
            return snapshot;
        }
        lock.lock();
        try {
            snapshot = cachedJwks;
            if (snapshot != null && (now - jwksFetchedAtEpochSeconds) < JWKS_TTL_SECONDS) {
                return snapshot;
            }
            HttpRequest request = HttpRequest.newBuilder(URI.create(config.jwksUrl()))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();
            HttpResponse<String> response;
            try {
                response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (Exception e) {
                throw new TokenValidationException("Failed to fetch JWKS: " + e.getMessage(), e);
            }
            if (response.statusCode() >= 400) {
                throw new TokenValidationException("Failed to fetch JWKS: HTTP " + response.statusCode());
            }
            JWKSet fetched;
            try {
                fetched = JWKSet.parse(response.body());
            } catch (ParseException e) {
                throw new TokenValidationException("Failed to parse JWKS response: " + e.getMessage(), e);
            }
            cachedJwks = fetched;
            jwksFetchedAtEpochSeconds = now;
            return fetched;
        } finally {
            lock.unlock();
        }
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
            throw new TokenValidationException("Invalid JWT_PUBLIC_KEY_PEM: " + e.getMessage(), e);
        }
    }
}
