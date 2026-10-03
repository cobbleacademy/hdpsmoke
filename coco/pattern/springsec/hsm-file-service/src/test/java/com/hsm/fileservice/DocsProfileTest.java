package com.hsm.fileservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsm.client.config.FipsBootstrap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The "docs" profile (chart docs.enabled): spec + Swagger UI on the API port under the
 * prefix, every URL relative so it works behind any VirtualService prefix, and nothing
 * docs-related left on the management port.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("docs")
class DocsProfileTest {

    static final String PREFIX = "/api/sensec/file/v1";

    private static final HttpClient HTTP = HttpClient.newHttpClient(); // does not follow redirects
    private static final Path ROOT;
    private static final FakeCoreService CORE;

    static {
        FipsBootstrap.register();
        try {
            ROOT = Files.createTempDirectory("hsm-file-service-docs");
            Files.createDirectories(ROOT.resolve("store"));
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
            kpg.initialize(2048);
            KeyPair kp = kpg.generateKeyPair();
            Files.writeString(ROOT.resolve("key.pem"), "-----BEGIN PRIVATE KEY-----\n"
                    + Base64.getMimeEncoder().encodeToString(kp.getPrivate().getEncoded()) + "\n-----END PRIVATE KEY-----\n");
            CORE = new FakeCoreService("/api/sensec/hsm/v1", kp.getPublic());
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry r) {
        r.add("hsm.file-service.core.base-url", CORE::baseUrl);
        r.add("hsm.file-service.core.app-id", () -> "docs-test");
        r.add("hsm.file-service.core.auth-mode", () -> "STATIC");
        r.add("hsm.file-service.core.static-token", () -> "t");
        r.add("hsm.file-service.core.private-key-pem-file", () -> ROOT.resolve("key.pem").toString());
        r.add("hsm.file-service.store.type", () -> "LOCAL");
        r.add("hsm.file-service.store.root", () -> ROOT.resolve("store").toString());
        r.add("hsm.file-service.access.allowed-path-prefixes", () -> "*");
        r.add("management.server.port", () -> 0);
    }

    @LocalServerPort
    int port;

    @LocalManagementPort
    int managementPort;

    @AfterAll
    static void stop() {
        CORE.close();
    }

    private HttpResponse<String> get(int p, String path) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + p + path)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void docsEntryRedirectsRelatively() throws Exception {
        HttpResponse<String> r = get(port, PREFIX + "/docs");
        assertEquals(302, r.statusCode());
        assertEquals("swagger-ui/index.html", r.headers().firstValue("Location").orElse(null));
        assertEquals(200, get(port, PREFIX + "/swagger-ui/index.html").statusCode());
    }

    @Test
    @SuppressWarnings("unchecked")
    void specOnApiPortIsRelative() throws Exception {
        HttpResponse<String> r = get(port, PREFIX + "/openapi");
        assertEquals(200, r.statusCode());
        Map<String, Object> spec = new ObjectMapper().readValue(r.body(), Map.class);
        assertEquals(List.of("/files/{path}"), List.copyOf(((Map<String, Object>) spec.get("paths")).keySet()),
                "only the file endpoint; /docs and springdoc's own endpoints are not part of the contract");
        assertEquals(".", ((List<Map<String, Object>>) spec.get("servers")).get(0).get("url"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void swaggerUiUrlsAreRelativeAndGetOnly() throws Exception {
        Map<String, Object> cfg = new ObjectMapper().readValue(get(port, PREFIX + "/openapi/swagger-config").body(), Map.class);
        assertEquals("../openapi/swagger-config", cfg.get("configUrl"));
        assertEquals("../openapi", cfg.get("url"));
        assertEquals(List.of("get"), cfg.get("supportedSubmitMethods"));
        // oauth2RedirectUrl is always absolute (springdoc builds it from the request) but is
        // used only by an OAuth2 login, and this API has no OAuth security scheme.
        String initializer = get(port, PREFIX + "/swagger-ui/swagger-initializer.js").body();
        assertFalse(initializer.contains(PREFIX), "initializer must not embed the internal prefix");
    }

    @Test
    void fileEndpointStillServedAndManagementPortHasNoDocs() throws Exception {
        HttpResponse<String> r = get(port, PREFIX + "/files/missing.pdf");
        assertEquals(404, r.statusCode());
        assertTrue(r.body().contains("FS-404-NOT-FOUND"), r.body());
        assertEquals(404, get(managementPort, "/actuator/openapi").statusCode(), "spec moved to the API port");
        assertEquals(404, get(managementPort, "/actuator/swagger-ui").statusCode());
        assertEquals(200, get(managementPort, "/actuator/health/readiness").statusCode());
    }
}
