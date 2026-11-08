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
public record FileServiceProperties(Server server, Core core, Store store, Access access, Delivery delivery,
                                    Limits limits, DekCache dekCache) {

    public FileServiceProperties {
        if (server == null) {
            server = new Server(null, null);
        }
    }

    /**
     * This service's OWN API -- inbound, what the BFF calls. Deliberately a separate group
     * from {@link Core}, which is the service this one calls (outbound): server.* /
     * FILE_SERVICE_* always means "me", core.* / HSM_CORE_* always means hsm-core-service.
     *
     * @param apiPrefix path the API is served under, version included (default
     *                  /api/sensec/file/v1); the file endpoint is {@code <apiPrefix>/files/{path}}.
     *                  Must start with "/" and must not end with "/".
     * @param apiV2Prefix the same endpoint as API v2 (default /api/sensec/file/v2): identical
     *                  file bytes and headers; the only JSON (the error body) is camelCase
     *                  ({@code errorCode}, {@code requestId}) instead of snake_case. Same rules.
     */
    public record Server(String apiPrefix, String apiV2Prefix) {
        public static final String DEFAULT_API_PREFIX = "/api/sensec/file/v1";
        public static final String DEFAULT_API_V2_PREFIX = "/api/sensec/file/v2";

        public Server {
            apiPrefix = checked("api-prefix", apiPrefix, DEFAULT_API_PREFIX);
            apiV2Prefix = checked("api-v2-prefix", apiV2Prefix, DEFAULT_API_V2_PREFIX);
            if (apiPrefix.equals(apiV2Prefix)) {
                throw new IllegalArgumentException("hsm.file-service.server.api-prefix and api-v2-prefix must differ, both are '"
                        + apiPrefix + "'");
            }
        }

        // Rejected rather than normalized: the request mapping is built from the raw value,
        // so anything trimmed here would no longer match what Spring actually maps.
        private static String checked(String name, String prefix, String fallback) {
            if (prefix == null || prefix.isBlank()) {
                return fallback;
            }
            if (!prefix.startsWith("/") || prefix.endsWith("/") || prefix.contains("//")
                    || prefix.contains("*") || prefix.contains("{") || prefix.matches(".*\\s.*")) {
                throw new IllegalArgumentException("hsm.file-service.server." + name + " must look like " + fallback
                        + " (leading /, no trailing /, no spaces, wildcards or placeholders), got: '" + prefix + "'");
            }
            return prefix;
        }

        /** True when the request URI is under the v2 prefix -- the error body is then camelCase. */
        public boolean isV2(String requestUri) {
            return requestUri != null && requestUri.startsWith(apiV2Prefix + "/");
        }
    }

    public enum AuthMode { STATIC, AZURE_AD, SELF_SIGNED_JWT, MTLS }

    /** Which hsm-core-service API version this service calls: V1 snake_case (default) or V2 camelCase. */
    public enum ApiVersion { V1, V2 }

    public enum StoreType { LOCAL, ADLS, AZURE_BLOB }

    /** How this service reaches hsm-core-service, as its own registered app_id. */
    public record Core(String baseUrl, String apiV1Prefix, String appId, AuthMode authMode,
                       String privateKeyPemFile, String staticToken, String azureTokenScope,
                       String signingKeyPemFile, String jwtAudience,
                       String mtlsCertPemFile, String mtlsKeyPemFile,
                       ApiVersion apiVersion, String apiV2Prefix) {
        public Core {
            if (apiVersion == null) {
                apiVersion = ApiVersion.V1;
            }
            if (apiV2Prefix == null || apiV2Prefix.isBlank()) {
                apiV2Prefix = "/api/sensec/hsm/v2";
            }
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
