package com.hsm.client.svc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.hsm.client.config.ClientProperties;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final ClientProperties.Svc config;

    public SvcClient(ClientProperties.Svc config) {
        this.config = config;
    }

    public record IssueItem(String key, String dataClassification) {
    }

    public record IssueResult(String key, String status, UUID edekId, String wrappedDekB64, String detail) {
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
                    .header("Authorization", "Bearer " + config.token())
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
