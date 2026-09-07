package com.hsm.client.svc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.hsm.client.crypto.TransportWrapper;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * HTTP client for SVC (hsm-core-service)'s POST /dek/issue and POST /dek/unwrap --
 * same request pattern already proven in BulkVsBatchBenchmark.postJson()
 * (java.net.http.HttpClient, Authorization: Bearer + X-App-ID headers), generalized
 * into typed request/response records instead of raw JSON node manipulation, since
 * this is the real client module, not a benchmark script.
 *
 * <p>Wire format is snake_case (SVC's spring.jackson.property-naming-strategy), so
 * this client's own ObjectMapper is configured the same way -- request/response
 * records serialize/deserialize directly, no manual field-name mapping needed.
 */
public class SvcClient {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    private final HttpClient http;
    private final SvcConfig config;
    /** Null only for authMode=MTLS -- that mode authenticates at the TLS handshake, not via a bearer token, so no Authorization header is sent at all. */
    private final TokenProvider tokenProvider;

    public SvcClient(SvcConfig config) {
        this.config = config;
        this.tokenProvider = switch (config.authMode() == null ? SvcConfig.AuthMode.STATIC : config.authMode()) {
            case STATIC -> new StaticTokenProvider(config.token());
            case AZURE_AD -> new AzureAdTokenProvider(config.azureTokenScope());
            case SELF_SIGNED_JWT -> new SelfSignedJwtTokenProvider(
                    TransportWrapper.parsePrivateKeyPem(config.signingPrivateKeyPem()),
                    config.appId(),
                    config.selfSignedAudience());
            case MTLS -> null;
        };
        this.http = buildHttpClient(config);
    }

    /**
     * Builds two independent, orthogonal TLS behaviors from config: whether this
     * client trusts SVC's own presented certificate (SVC_INSECURE_TLS env var --
     * deliberately not a config field, so it can never be checked into a job.yml
     * and silently ship; testing/throwaway-deployment escape hatch only, prefer
     * importing the real certificate into the JVM's trust store instead), and
     * whether this client presents its own certificate during the handshake
     * (authMode=MTLS -- config.mtlsCertPem/mtlsKeyPem). Either, both, or neither
     * may apply to a given SvcClient instance.
     */
    private static HttpClient buildHttpClient(SvcConfig config) {
        HttpClient.Builder builder = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5));
        boolean insecure = Boolean.parseBoolean(System.getenv("SVC_INSECURE_TLS"));
        boolean mtls = config.authMode() == SvcConfig.AuthMode.MTLS;
        if (!insecure && !mtls) {
            return builder.build();
        }
        try {
            KeyManager[] keyManagers = mtls
                    ? MtlsSupport.buildKeyManagers(config.mtlsCertPem(), config.mtlsKeyPem())
                    : null;
            TrustManager[] trustManagers = null;
            if (insecure) {
                System.err.println("WARNING: SVC_INSECURE_TLS=true -- TLS certificate verification to SVC is DISABLED. Testing only.");
                trustManagers = new TrustManager[]{ new X509TrustManager() {
                    public void checkClientTrusted(X509Certificate[] chain, String authType) { }
                    public void checkServerTrusted(X509Certificate[] chain, String authType) { }
                    public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                } };
            }
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagers, trustManagers, new SecureRandom());
            builder.sslContext(sslContext);
            if (insecure) {
                SSLParameters sslParameters = new SSLParameters();
                sslParameters.setEndpointIdentificationAlgorithm(""); // also skip hostname verification
                builder.sslParameters(sslParameters);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to configure TLS (SVC_INSECURE_TLS and/or authMode=MTLS)", e);
        }
        return builder.build();
    }

    /** name is optional -- see SvcConfig's javadoc / ClientProperties.Db.ColumnMapping.dekName's javadoc for what it does. */
    public record IssueItem(String key, String dataClassification, String name) {
    }

    /**
     * ownerAppId is the record's permanent owner -- NOT necessarily the
     * calling app once a grant-authorized cross-app dek_name reuse is in
     * play. Callers MUST use this (never their own configured appId) as the
     * AES-GCM AAD for local encrypt; using the caller's own identity for a
     * cross-app reuse silently produces a ciphertext nothing can ever
     * decrypt again -- a real, confirmed bug fixed on the hsm-core-service
     * side of this exact response (EncryptionService.ResolvedDek's javadoc
     * has the full story) the same round this field was added here.
     */
    public record IssueResult(String key, String status, UUID edekId, String wrappedDekB64, String ownerAppId, String detail, boolean reused) {
    }

    public record UnwrapItem(String key, UUID edekId) {
    }

    /** ownerAppId is the record's permanent owner -- required as the AES-GCM AAD for local decrypt; see IssueResult's javadoc for the identical reasoning on the encrypt side. */
    public record UnwrapResult(String key, String status, UUID edekId, String wrappedDekB64, String ownerAppId, String detail) {
    }

    private record ItemsRequest<T>(List<T> items) {
    }

    private record ItemsResponse<T>(List<T> items) {
    }

    public List<IssueResult> issue(List<IssueItem> items) {
        return post("/dek/issue", new ItemsRequest<>(items), IssueResult.class);
    }

    public List<UnwrapResult> unwrap(List<UnwrapItem> items) {
        return post("/dek/unwrap", new ItemsRequest<>(items), UnwrapResult.class);
    }

    private <T, R> List<R> post(String path, ItemsRequest<T> body, Class<R> resultType) {
        try {
            String json = MAPPER.writeValueAsString(body);
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(URI.create(config.baseUrl() + config.apiV1Prefix() + path))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .header("X-App-ID", config.appId());
            // tokenProvider is null only for authMode=MTLS -- identity was already
            // established at the TLS handshake, no bearer token needed or sent.
            if (tokenProvider != null) {
                requestBuilder.header("Authorization", "Bearer " + tokenProvider.getBearerToken());
            }
            HttpRequest request = requestBuilder.POST(HttpRequest.BodyPublishers.ofString(json)).build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 300) {
                throw new SvcClientException("POST " + path + " -> HTTP " + response.statusCode() + ": " + response.body());
            }
            var responseType = MAPPER.getTypeFactory().constructParametricType(ItemsResponse.class, resultType);
            ItemsResponse<R> parsed = MAPPER.readValue(response.body(), responseType);
            return parsed.items();
        } catch (SvcClientException e) {
            throw e;
        } catch (Exception e) {
            throw new SvcClientException("POST " + path + " failed: " + e.getMessage(), e);
        }
    }

    public static class SvcClientException extends RuntimeException {
        public SvcClientException(String message) {
            super(message);
        }

        public SvcClientException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
