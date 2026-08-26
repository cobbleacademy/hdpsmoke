package com.hsm.client.svc;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import java.security.PrivateKey;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Locally signs a short-lived bearer assertion (RFC 7523-style) with this
 * app's own private key instead of acquiring a token from an external IdP --
 * built for callers that find OAuth2 client-credentials/JWT-renewal
 * machinery operationally painful but can manage a one-time keypair.
 * "Renewal" here is pure local computation (re-sign a small JWT), never a
 * network call -- see hsm-core-service's SelfSignedAppKeyJwtValidator, which
 * verifies the signature against this app's registered
 * signing_public_key_pem (or public_key_pem on the legacy one-keypair
 * fallback).
 *
 * <p>Caches the signed token and only re-signs once within
 * REFRESH_MARGIN of expiry -- signing is cheap, but there's no reason to
 * re-sign on every single call in a tight loop (e.g. DbBulkJob's per-page
 * fetch-issue-encrypt-insert cycle). Thread-safe: hsm-bulk-client's
 * db.parallelism/file.parallelism can drive multiple workers sharing one
 * SvcClient/TokenProvider instance.
 */
public class SelfSignedJwtTokenProvider implements TokenProvider {

    /** Well under SelfSignedAppKeyJwtValidator's server-side MAX_TTL (5 min). */
    private static final Duration TOKEN_TTL = Duration.ofMinutes(2);
    private static final Duration REFRESH_MARGIN = Duration.ofSeconds(15);

    private final PrivateKey signingKey;
    private final String appId;
    private final String audience;
    private final ReentrantLock lock = new ReentrantLock();

    private volatile String cachedToken;
    private volatile Instant cachedExpiry = Instant.EPOCH;

    public SelfSignedJwtTokenProvider(PrivateKey signingKey, String appId, String audience) {
        this.signingKey = signingKey;
        this.appId = appId;
        this.audience = audience;
    }

    @Override
    public String getBearerToken() {
        Instant now = Instant.now();
        if (cachedToken != null && now.isBefore(cachedExpiry.minus(REFRESH_MARGIN))) {
            return cachedToken;
        }
        lock.lock();
        try {
            if (cachedToken != null && now.isBefore(cachedExpiry.minus(REFRESH_MARGIN))) {
                return cachedToken;
            }
            return mintToken(now);
        } finally {
            lock.unlock();
        }
    }

    private String mintToken(Instant now) {
        Instant expiry = now.plus(TOKEN_TTL);
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject(appId)
                .issuer(appId)
                .audience(audience)
                .issueTime(Date.from(now))
                .expirationTime(Date.from(expiry))
                .jwtID(UUID.randomUUID().toString())
                .build();
        SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.RS256), claims);
        JWSSigner signer = new RSASSASigner(signingKey);
        try {
            jwt.sign(signer);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to sign self-issued bearer JWT", e);
        }
        String serialized = jwt.serialize();
        cachedToken = serialized;
        cachedExpiry = expiry;
        return serialized;
    }
}
