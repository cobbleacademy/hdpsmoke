package com.hsm.client;

import com.hsm.client.config.FipsBootstrap;
import com.hsm.client.crypto.DekManager;
import com.hsm.client.crypto.TransportWrapper;
import com.hsm.client.svc.SvcClient;
import com.hsm.client.svc.SvcConfig;

import javax.crypto.AEADBadTagException;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stateful, embeddable client for hsm-core-service's bulk DEK endpoints
 * (POST /dek/issue, POST /dek/unwrap) -- the "other JVM applications import
 * this and call encrypt/decrypt directly" entry point this module exists
 * for. The caller owns this object's lifecycle: build one, keep it around
 * for as long as encrypt/decrypt calls are needed (it holds a DEK cache and
 * an HTTP connection, so it is not meant to be built fresh per call), and
 * {@link #close()} it when done -- same contract as any pooled resource.
 *
 * <p><b>DEK lifecycle, why this can't be stateless static methods:</b> a
 * caller repeatedly encrypting under the same {@code dekName} needs to reuse
 * that DEK, not mint a new one (and pay a fresh /dek/issue round-trip plus
 * an RSA-OAEP unwrap) on every single call -- the same reasoning
 * hsm-bulk-client's DbBulkJob/FileBulkJob already apply for their own bulk
 * runs. This class keeps its own, independent encrypt-side (by dekName) and
 * decrypt-side (by edek_id) caches -- separate instances of this client
 * never share cached key material.
 *
 * <p><b>Ciphertext format:</b> {@link #encrypt} returns the exact same
 * {@code "v1...."} packed-token format hsm-core-service's own /encrypt
 * produces -- directly decryptable via hsm-core-service's /decrypt with no
 * awareness this client exists, and vice versa (see FileBulkJob's own class
 * javadoc and CoreBulkFileInteropTest for the same interoperability
 * guarantee this class relies on).
 *
 * <p>Not thread-hostile -- the internal caches are concurrent-safe, so one
 * instance may be shared across threads -- but each instance is a single
 * logical connection to one SVC deployment as one app_id; use a separate
 * instance per (appId, SVC deployment) pair if an embedding application
 * needs to act as more than one.
 *
 * <p>Registers the BC-FIPS security provider itself (see FipsBootstrap) the
 * first time this class is loaded -- an embedding application should never
 * need to know that DekManager/TransportWrapper are BC-FIPS-backed
 * internally, let alone remember to register the provider before using
 * this. hsm-bulk-client's own CLI did this via a static initializer on its
 * entry point (ClientApplication); an embedded caller has no equivalent
 * single entry point, so this class takes over that responsibility itself.
 */
public class HsmCryptoClient implements AutoCloseable {

    static {
        FipsBootstrap.register();
    }

    private final SvcClient svcClient;
    private final PrivateKey privateKey;
    private final String appId;

    private final Map<String, CachedDek> encryptCacheByName = new ConcurrentHashMap<>();
    private final Map<UUID, byte[]> decryptCacheByEdekId = new ConcurrentHashMap<>();

    private volatile boolean closed = false;

    private HsmCryptoClient(SvcClient svcClient, PrivateKey privateKey, String appId) {
        this.svcClient = svcClient;
        this.privateKey = privateKey;
        this.appId = appId;
    }

    public static Builder builder() {
        return new Builder();
    }

    private record CachedDek(UUID edekId, byte[] dek) {
    }

    public static class HsmCryptoClientException extends RuntimeException {
        public HsmCryptoClientException(String message) {
            super(message);
        }

        public HsmCryptoClientException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // ---- encrypt ----

    /** Mints a fresh DEK for this call -- unset-dekName default, DEK-per-call, unchanged. */
    public String encrypt(byte[] plaintext) {
        return encrypt(plaintext, null, null);
    }

    /** Reuses the current DEK for dekName if one has already been issued by this client instance, else mints and caches one. */
    public String encrypt(byte[] plaintext, String dekName) {
        return encrypt(plaintext, dekName, null);
    }

    public String encrypt(byte[] plaintext, String dekName, String dataClassification) {
        checkOpen();
        boolean named = dekName != null && !dekName.isBlank();
        CachedDek cached = named
                ? encryptCacheByName.computeIfAbsent(dekName, name -> issueOne(name, dataClassification))
                : issueOne(null, dataClassification);

        DekManager.EncryptResult encrypted = DekManager.encrypt(plaintext, cached.dek(), appId);
        return DekManager.packToken(cached.edekId(), encrypted.iv(), encrypted.tag(), encrypted.ciphertext());
    }

    /** UTF-8 convenience overload of {@link #encrypt(byte[])}. */
    public String encrypt(String plaintext) {
        return encrypt(plaintext.getBytes(StandardCharsets.UTF_8), null, null);
    }

    /** UTF-8 convenience overload of {@link #encrypt(byte[], String)}. */
    public String encrypt(String plaintext, String dekName) {
        return encrypt(plaintext.getBytes(StandardCharsets.UTF_8), dekName, null);
    }

    /** UTF-8 convenience overload of {@link #encrypt(byte[], String, String)}. */
    public String encrypt(String plaintext, String dekName, String dataClassification) {
        return encrypt(plaintext.getBytes(StandardCharsets.UTF_8), dekName, dataClassification);
    }

    private CachedDek issueOne(String dekName, String dataClassification) {
        List<SvcClient.IssueResult> results = svcClient.issue(
                List.of(new SvcClient.IssueItem("encrypt", dataClassification, dekName)));
        SvcClient.IssueResult result = results.get(0);
        if (!"success".equals(result.status())) {
            throw new HsmCryptoClientException("dek/issue failed: " + result.detail());
        }
        byte[] dek = TransportWrapper.unwrap(Base64.getDecoder().decode(result.wrappedDekB64()), privateKey);
        return new CachedDek(result.edekId(), dek);
    }

    // ---- decrypt ----

    /**
     * Single form, not multiple overloads -- unlike encrypt (which has a real
     * fresh-vs-reuse distinction), the packed ciphertext token already carries
     * everything (edek_id, iv, tag, ciphertext) needed to decrypt it, the same
     * design reason hsm-core-service's own /decrypt takes one field.
     */
    public byte[] decrypt(String ciphertextToken) {
        checkOpen();
        DekManager.UnpackedToken unpacked = DekManager.unpackToken(ciphertextToken);
        byte[] dek = decryptCacheByEdekId.computeIfAbsent(unpacked.edekId(), this::unwrapOne);
        try {
            return DekManager.decrypt(unpacked.ciphertext(), unpacked.tag(), unpacked.iv(), dek, appId);
        } catch (AEADBadTagException e) {
            throw new HsmCryptoClientException("ciphertext authentication failed: tampered or corrupt", e);
        }
    }

    /** UTF-8 convenience overload of {@link #decrypt(String)}. */
    public String decryptToString(String ciphertextToken) {
        return new String(decrypt(ciphertextToken), StandardCharsets.UTF_8);
    }

    private byte[] unwrapOne(UUID edekId) {
        List<SvcClient.UnwrapResult> results = svcClient.unwrap(List.of(new SvcClient.UnwrapItem("decrypt", edekId)));
        SvcClient.UnwrapResult result = results.get(0);
        if (!"success".equals(result.status())) {
            throw new HsmCryptoClientException("dek/unwrap failed: " + result.detail());
        }
        return TransportWrapper.unwrap(Base64.getDecoder().decode(result.wrappedDekB64()), privateKey);
    }

    // ---- lifecycle ----

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("HsmCryptoClient is closed");
        }
    }

    /** Zeroes every cached DEK. Idempotent; safe to call more than once. */
    @Override
    public void close() {
        closed = true;
        encryptCacheByName.values().forEach(c -> DekManager.zeroDek(c.dek()));
        encryptCacheByName.clear();
        decryptCacheByEdekId.values().forEach(DekManager::zeroDek);
        decryptCacheByEdekId.clear();
    }

    // ---- builder ----

    public static final class Builder {
        private String baseUrl;
        private String apiV1Prefix = "/api/sensec/hsm/v1";
        private String appId;
        private String privateKeyPem;
        private SvcConfig.AuthMode authMode;
        private String staticToken;
        private String azureTokenScope;
        private String signingKeyPem;
        private String selfSignedAudience;

        private Builder() {
        }

        public Builder baseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
            return this;
        }

        /** Defaults to "/api/sensec/hsm/v1" -- must match SVC's own hsm.service.api-v1-prefix. */
        public Builder apiV1Prefix(String apiV1Prefix) {
            this.apiV1Prefix = apiV1Prefix;
            return this;
        }

        public Builder appId(String appId) {
            this.appId = appId;
            return this;
        }

        /** PKCS#8 PEM -- the private half of the public key registered on app_registrations.public_key_pem for appId. Used locally to unwrap what SVC returns; never sent anywhere. Required regardless of auth mode. */
        public Builder privateKeyPem(String privateKeyPem) {
            this.privateKeyPem = privateKeyPem;
            return this;
        }

        /** A fixed bearer token, sent as-is on every call. Fine for demo/mock-mode tokens (never expire); a real Azure AD JWT here would expire mid-session. */
        public Builder staticToken(String token) {
            this.authMode = SvcConfig.AuthMode.STATIC;
            this.staticToken = token;
            return this;
        }

        /** Acquires a fresh Azure AD access token before each call via Workload Identity/Managed Identity/DefaultAzureCredential -- see AzureAdTokenProvider. scope must match SVC's own Azure AD app registration's exposed scope. */
        public Builder azureAdToken(String scope) {
            this.authMode = SvcConfig.AuthMode.AZURE_AD;
            this.azureTokenScope = scope;
            return this;
        }

        /**
         * Locally signs a short-lived bearer assertion with signingKeyPem instead of
         * acquiring a token from an external IdP -- "renewal" is pure local
         * computation, never a network call. See SelfSignedJwtTokenProvider.
         * signingKeyPem is PKCS#8 PEM, the private half of the key registered on
         * app_registrations.signing_public_key_pem (or public_key_pem, on the legacy
         * one-keypair fallback). audience must match SVC's own hsm.jwt.audience;
         * null/blank defaults to "hsm-core-service".
         */
        public Builder selfSignedJwt(String signingKeyPem, String audience) {
            this.authMode = SvcConfig.AuthMode.SELF_SIGNED_JWT;
            this.signingKeyPem = signingKeyPem;
            this.selfSignedAudience = audience;
            return this;
        }

        public Builder selfSignedJwt(String signingKeyPem) {
            return selfSignedJwt(signingKeyPem, null);
        }

        public HsmCryptoClient build() {
            if (baseUrl == null || baseUrl.isBlank()) {
                throw new IllegalStateException("baseUrl is required");
            }
            if (appId == null || appId.isBlank()) {
                throw new IllegalStateException("appId is required");
            }
            if (authMode == null) {
                throw new IllegalStateException("exactly one of staticToken(...)/azureAdToken(...)/selfSignedJwt(...) is required");
            }
            if (privateKeyPem == null || privateKeyPem.isBlank()) {
                throw new IllegalStateException("privateKeyPem is required -- unwraps what SVC returns from /dek/issue and /dek/unwrap");
            }
            PrivateKey privateKey = TransportWrapper.parsePrivateKeyPem(privateKeyPem);
            SvcConfig config = new SvcConfig(
                    baseUrl, apiV1Prefix, appId, authMode,
                    staticToken, azureTokenScope,
                    1, // dekBatchMaxItems -- unused: this client always issues exactly one item per call
                    privateKeyPem, signingKeyPem, selfSignedAudience);
            SvcClient svcClient = new SvcClient(config);
            return new HsmCryptoClient(svcClient, privateKey, appId);
        }
    }
}
