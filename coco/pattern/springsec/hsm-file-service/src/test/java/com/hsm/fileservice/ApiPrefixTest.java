package com.hsm.fileservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsm.client.config.FipsBootstrap;
import com.hsm.fileservice.config.FileServiceProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.boot.test.web.server.LocalServerPort;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * hsm.file-service.server.api-prefix (FILE_SERVICE_API_PREFIX) is this service's OWN
 * prefix: the endpoint moves with it, the default one stops answering, and the
 * published spec follows it. Separate from core.api-v1-prefix, the prefix of the
 * service this one calls.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ApiPrefixTest {

    static final String CUSTOM = "/api/acme/docs/v2";

    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final Path ROOT;
    private static final FakeCoreService CORE;

    static {
        FipsBootstrap.register();
        try {
            ROOT = Files.createTempDirectory("hsm-file-service-prefix");
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
        r.add("hsm.file-service.server.api-prefix", () -> CUSTOM);
        r.add("hsm.file-service.core.base-url", CORE::baseUrl);
        r.add("hsm.file-service.core.app-id", () -> "prefix-test");
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
    void endpointLivesUnderTheConfiguredPrefix() throws Exception {
        // Our own 404 (JSON error code) proves the request reached FileController under CUSTOM.
        HttpResponse<String> r = get(port, CUSTOM + "/files/missing.pdf");
        assertEquals(404, r.statusCode());
        assertTrue(r.body().contains("FS-404-NOT-FOUND"), r.body());
    }

    @Test
    void defaultPrefixNoLongerAnswers() throws Exception {
        HttpResponse<String> r = get(port, FileServiceProperties.Server.DEFAULT_API_PREFIX + "/files/missing.pdf");
        assertEquals(404, r.statusCode());
        assertFalse(r.body().contains("FS-404-NOT-FOUND"), "no file endpoint is mapped at the default prefix");
    }

    @Test
    void noDocsOnTheApiPortWithoutTheDocsProfile() throws Exception {
        assertEquals(404, get(port, CUSTOM + "/docs").statusCode());
        assertEquals(404, get(port, CUSTOM + "/openapi").statusCode());
        assertEquals(404, get(port, CUSTOM + "/swagger-ui/index.html").statusCode());
    }

    @Test
    @SuppressWarnings("unchecked")
    void publishedSpecFollowsThePrefix() throws Exception {
        Map<String, Object> spec = new ObjectMapper().readValue(get(managementPort, "/actuator/openapi").body(), Map.class);
        assertEquals(List.of("/files/{path}"), List.copyOf(((Map<String, Object>) spec.get("paths")).keySet()));
        Map<String, Object> server = ((List<Map<String, Object>>) spec.get("servers")).get(0);
        assertEquals(CUSTOM, ((Map<String, Map<String, Object>>) server.get("variables")).get("apiPrefix").get("default"));
    }

    @Test
    void malformedPrefixesAreRejectedAtStartup() {
        for (String bad : List.of("api/no-leading-slash", "/trailing/", "/a//b", "/a/*", "/a/{x}", "/a b")) {
            assertThrows(IllegalArgumentException.class, () -> new FileServiceProperties.Server(bad), bad);
        }
        assertEquals(FileServiceProperties.Server.DEFAULT_API_PREFIX, new FileServiceProperties.Server(null).apiPrefix());
    }
}
