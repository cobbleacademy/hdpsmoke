package com.hsm.fileservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

/**
 * Everything a consumer can configure, bound from {@code hsm.file-service.*}. This is
 * the service's public contract -- consumers never see the code, only the Helm
 * chart's values.yaml, which maps 1:1 onto these properties (documented in
 * java/docs/FILE_SERVICE.md "Configuration reference"). Adding a property is a minor
 * version bump; renaming or removing one is a major one.
 */
@ConfigurationProperties("hsm.file-service")
public record FileServiceProperties(Core core, Store store, Access access, Delivery delivery,
                                    Limits limits, DekCache dekCache) {

    public enum AuthMode { STATIC, AZURE_AD, SELF_SIGNED_JWT, MTLS }

    public enum StoreType { LOCAL, ADLS, AZURE_BLOB }

    /** How this service reaches hsm-core-service, as its own registered app_id. */
    public record Core(String baseUrl, String apiV1Prefix, String appId, AuthMode authMode,
                       String privateKeyPemFile, String staticToken, String azureTokenScope,
                       String signingKeyPemFile, String jwtAudience,
                       String mtlsCertPemFile, String mtlsKeyPemFile) {
        public Core {
            if (apiV1Prefix == null || apiV1Prefix.isBlank()) {
                apiV1Prefix = "/api/sensec/hsm/v1";
            }
            if (authMode == null) {
                authMode = AuthMode.AZURE_AD;
            }
        }
    }

    /** accountKey is a dev-only escape hatch (same as hsm-bulk-client's); leave unset in real deployments. */
    public record Store(StoreType type, String root, String accountKey) {
    }

    /**
     * Option A trust model (FILE_SERVICE.md "Access model"): the BFF decides which user
     * may see which file; this service enforces who may call it and which paths exist.
     *
     * @param allowedPathPrefixes     required, non-empty. Path prefixes (at a "/" boundary) that may be served; "*" allows every path.
     * @param requireExpectedFileId   true: every request must carry X-Expected-File-Id (only v2 files can then be served).
     * @param trustedCallerSpiffeIds  optional defence in depth on top of the Istio AuthorizationPolicy: if set, the
     *                                immediate caller's SPIFFE id from Istio's x-forwarded-client-cert must be listed.
     *                                Only enable inside the mesh -- outside it the header is caller-controlled.
     */
    public record Access(List<String> allowedPathPrefixes, boolean requireExpectedFileId,
                         List<String> trustedCallerSpiffeIds) {
        public Access {
            allowedPathPrefixes = allowedPathPrefixes == null ? List.of() : List.copyOf(allowedPathPrefixes);
            trustedCallerSpiffeIds = trustedCallerSpiffeIds == null ? List.of() : List.copyOf(trustedCallerSpiffeIds);
        }
    }

    /**
     * @param bufferThresholdBytes  stored (encrypted) size at or below which a file is fully decrypted and verified
     *                              before the first byte is sent. Default 21.5 MiB stored ~= 16 MiB plaintext.
     * @param maxBufferedRequests   concurrent buffered downloads; beyond this a small file is streamed instead.
     * @param bufferAcquireTimeout  how long to wait for a buffer slot before falling back to streaming.
     * @param contentDisposition    "inline" (default, UI renders it) or "attachment" (browser downloads it).
     */
    public record Delivery(long bufferThresholdBytes, int maxBufferedRequests, Duration bufferAcquireTimeout,
                           String contentDisposition) {
        public Delivery {
            if (bufferThresholdBytes < 0) {
                bufferThresholdBytes = 0;
            }
            if (maxBufferedRequests < 1) {
                maxBufferedRequests = 1;
            }
            if (bufferAcquireTimeout == null) {
                bufferAcquireTimeout = Duration.ofMillis(100);
            }
            if (!"attachment".equalsIgnoreCase(contentDisposition)) {
                contentDisposition = "inline";
            }
        }
    }

    /** Hard caps on one frame / one decoded chunk, whatever a file's header claims. */
    public record Limits(int maxFrameBytes, int maxChunkPlaintextBytes) {
    }

    /** Unwrapped-DEK cache. TTL is also the revocation lag: a shredded or revoked key can keep serving for up to ttl + 60 s. */
    public record DekCache(Duration ttl, int maxSize) {
        public DekCache {
            if (ttl == null) {
                ttl = Duration.ofMinutes(15);
            }
            if (maxSize < 1) {
                maxSize = 200;
            }
        }
    }
}
