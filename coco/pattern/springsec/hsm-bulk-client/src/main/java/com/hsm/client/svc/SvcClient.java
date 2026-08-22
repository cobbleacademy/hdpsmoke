package com.hsm.client.svc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.hsm.client.config.ClientProperties;

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
 * HTTP client for SVC (hsm-bulk-service)'s POST /dek/issue and POST /dek/unwrap --
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

    private final HttpClient http = buildHttpClient();

    /**
     * TLS certificate verification is only ever skipped when SVC_INSECURE_TLS=true
     * is set in the environment -- deliberately an env var, not a ClientProperties
     * config field, so it can never be checked into a job.yml and silently ship.
     * Testing/throwaway-deployment escape hatch only (e.g. proof-ui against a
     * self-signed remote SVC): prefer importing the real certificate into the
     * JVM's trust store instead whenever that's an option.
     */
    private static HttpClient buildHttpClient() {
        HttpClient.Builder builder = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5));
        if (Boolean.parseBoolean(System.getenv("SVC_INSECURE_TLS"))) {
            System.err.println("WARNING: SVC_INSECURE_TLS=true -- TLS certificate verification to SVC is DISABLED. Testing only.");
            try {
                TrustManager[] trustAll = { new X509TrustManager() {
                    public void checkClientTrusted(X509Certificate[] chain, String authType) { }
                    public void checkServerTrusted(X509Certificate[] chain, String authType) { }
                    public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                } };
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, trustAll, new SecureRandom());
                SSLParameters sslParameters = new SSLParameters();
                sslParameters.setEndpointIdentificationAlgorithm(""); // also skip hostname verification
                builder.sslContext(sslContext).sslParameters(sslParameters);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to configure SVC_INSECURE_TLS", e);
            }
        }
        return builder.build();
    }
    private final ClientProperties.Svc config;
    private final TokenProvider tokenProvider;

    public SvcClient(ClientProperties.Svc config) {
        this.config = config;
        this.tokenProvider = switch (config.authMode() == null ? ClientProperties.Svc.AuthMode.STATIC : config.authMode()) {
            case STATIC -> new StaticTokenProvider(config.token());
            case AZURE_AD -> new AzureAdTokenProvider(config.azureTokenScope());
        };
    }

    /** name is optional -- see ClientProperties.Db.ColumnMapping.dekName's javadoc for what it does. */
    public record IssueItem(String key, String dataClassification, String name) {
    }

    public record IssueResult(String key, String status, UUID edekId, String wrappedDekB64, String detail, boolean reused) {
    }

    public record UnwrapItem(String key, UUID edekId) {
    }

    public record UnwrapResult(String key, String status, UUID edekId, String wrappedDekB64, String detail) {
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
            HttpRequest request = HttpRequest.newBuilder(URI.create(config.baseUrl() + config.apiV1Prefix() + path))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + tokenProvider.getBearerToken())
                    .header("X-App-ID", config.appId())
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build();
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
