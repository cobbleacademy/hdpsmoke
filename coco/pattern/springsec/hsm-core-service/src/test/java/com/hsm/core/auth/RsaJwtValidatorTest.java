package com.hsm.core.auth;

import com.hsm.core.config.HsmProperties;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.util.Base64;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers the comma-separated multi-value audience/issuer support added to
 * HsmProperties.Jwt/RsaJwtValidator -- a single Azure AD app registration
 * legitimately produces tokens with different aud/iss depending on which
 * credential path acquired them (v1.0 vs v2.0 endpoint), so a real deployment
 * needs to accept more than one exact value for each. Signs real RS256 JWTs
 * with a throwaway in-test keypair (publicKeyPem configured directly, so
 * resolveKey() never needs a live JWKS endpoint) rather than mocking
 * JwtValidator's internals.
 */
class RsaJwtValidatorTest {

    private static final KeyPair KEY_PAIR = generateKeyPair();

    private static KeyPair generateKeyPair() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            return generator.generateKeyPair();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String publicKeyPem() {
        String base64 = Base64.getEncoder().encodeToString(KEY_PAIR.getPublic().getEncoded());
        StringBuilder pem = new StringBuilder("-----BEGIN PUBLIC KEY-----\n");
        for (int i = 0; i < base64.length(); i += 64) {
            pem.append(base64, i, Math.min(i + 64, base64.length())).append('\n');
        }
        return pem.append("-----END PUBLIC KEY-----\n").toString();
    }

    private static String signedToken(String audience, String issuer) {
        try {
            JWTClaimsSet claims = new JWTClaimsSet.Builder()
                    .audience(audience)
                    .issuer(issuer)
                    .subject("test-app")
                    .claim("app_id", "test-app")
                    .expirationTime(new Date(System.currentTimeMillis() + 300_000))
                    .build();
            SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.RS256), claims);
            jwt.sign(new RSASSASigner((RSAPrivateKey) KEY_PAIR.getPrivate()));
            return jwt.serialize();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static RsaJwtValidator validatorWith(String audienceConfig, String issuerConfig) {
        return new RsaJwtValidator(new HsmProperties.Jwt(publicKeyPem(), "", audienceConfig, issuerConfig));
    }

    @Test
    void acceptsTokenMatchingSingleConfiguredValue_backwardCompatible() throws Exception {
        RsaJwtValidator validator = validatorWith("hsm-core-service", "https://issuer.example/tenant");
        String token = signedToken("hsm-core-service", "https://issuer.example/tenant");

        var claims = validator.validate(token);

        assertEquals("test-app", claims.get("app_id"));
    }

    @Test
    void acceptsV1StyleAudienceAndIssuer_whenBothListedAlongsideV2Style() throws Exception {
        RsaJwtValidator validator = validatorWith(
                "https://sts.windows.net/tenant-guid/aud,api://hsm-core-service",
                "https://sts.windows.net/tenant-guid/,https://login.microsoftonline.com/tenant-guid/v2.0");
        String v1Token = signedToken("https://sts.windows.net/tenant-guid/aud", "https://sts.windows.net/tenant-guid/");

        var claims = validator.validate(v1Token);

        assertEquals("test-app", claims.get("app_id"));
    }

    @Test
    void acceptsV2StyleAudienceAndIssuer_whenBothListedAlongsideV1Style() throws Exception {
        RsaJwtValidator validator = validatorWith(
                "https://sts.windows.net/tenant-guid/aud,api://hsm-core-service",
                "https://sts.windows.net/tenant-guid/,https://login.microsoftonline.com/tenant-guid/v2.0");
        String v2Token = signedToken("api://hsm-core-service", "https://login.microsoftonline.com/tenant-guid/v2.0");

        var claims = validator.validate(v2Token);

        assertEquals("test-app", claims.get("app_id"));
    }

    @Test
    void rejectsAudienceNotInList() {
        RsaJwtValidator validator = validatorWith("api://hsm-core-service", "https://issuer.example/tenant");
        String token = signedToken("api://some-other-resource", "https://issuer.example/tenant");

        TokenValidationException ex = assertThrows(TokenValidationException.class, () -> validator.validate(token));
        assertEquals("Invalid audience", ex.getMessage());
    }

    @Test
    void rejectsIssuerNotInList() {
        RsaJwtValidator validator = validatorWith("api://hsm-core-service", "https://issuer.example/tenant");
        String token = signedToken("api://hsm-core-service", "https://untrusted-issuer.example/other-tenant");

        TokenValidationException ex = assertThrows(TokenValidationException.class, () -> validator.validate(token));
        assertEquals("Invalid issuer", ex.getMessage());
    }

    @Test
    void rejectsEverything_whenAudienceOrIssuerUnconfigured_sameAsOldSingleValueDefault() {
        RsaJwtValidator validator = validatorWith("", "");
        String token = signedToken("api://hsm-core-service", "https://issuer.example/tenant");

        assertThrows(TokenValidationException.class, () -> validator.validate(token));
    }
}
