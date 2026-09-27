package com.hsm.fileservice;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsm.client.config.FipsBootstrap;
import com.hsm.fileservice.web.ErrorCode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Keeps the published contract honest. Consumers never see this code -- they get
 * helm/hsm-file-service/openapi.yaml with the chart -- so that file must be exactly
 * what the running service generates. If this test fails after an intentional API
 * change, review target/generated-openapi.yaml and copy it over the committed file
 * (or run with -Dopenapi.update=true, which writes it for you), and bump the chart
 * version per FILE_SERVICE.md "Versioning and compatibility".
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OpenApiContractTest {

    static final Path COMMITTED = Path.of("..", "..", "helm", "hsm-file-service", "openapi.yaml").toAbsolutePath().normalize();
    static final Path GENERATED = Path.of("target", "generated-openapi.yaml");

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final Path ROOT;
    private static final FakeCoreService CORE;

    static {
        FipsBootstrap.register();
        try {
            ROOT = Files.createTempDirectory("hsm-file-service-oas");
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
        r.add("hsm.file-service.core.app-id", () -> "oas-test");
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

    @SuppressWarnings("unchecked")
    private Map<String, Object> generated() throws Exception {
        HttpResponse<String> r = get(managementPort, "/actuator/openapi");
        assertEquals(200, r.statusCode(), "spec must be served on the management port");
        return JSON.readValue(r.body(), Map.class);
    }

    @Test
    void committedSpecMatchesWhatTheServiceGenerates() throws Exception {
        Map<String, Object> generated = generated();
        DumperOptions opts = new DumperOptions();
        opts.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        opts.setWidth(120);
        String yaml = "# GENERATED from hsm-file-service by OpenApiContractTest -- do not edit by hand.\n"
                + "# This is the published contract shipped with the chart; see FILE_SERVICE.md \"API\".\n"
                + new Yaml(opts).dump(generated);
        Files.createDirectories(GENERATED.getParent());
        Files.writeString(GENERATED, yaml, StandardCharsets.UTF_8);
        if (Boolean.getBoolean("openapi.update")) {
            Files.writeString(COMMITTED, yaml, StandardCharsets.UTF_8);
        }

        if (!Files.exists(COMMITTED)) {
            fail("No committed spec at " + COMMITTED + ". Review " + GENERATED.toAbsolutePath() + " and copy it there.");
        }
        Object committed = new Yaml().load(Files.readString(COMMITTED));
        JsonNode want = JSON.valueToTree(committed);
        JsonNode got = JSON.valueToTree(generated);
        if (!want.equals(got)) {
            fail("helm/hsm-file-service/openapi.yaml is out of date with the service. If the API change is intentional, "
                    + "review " + GENERATED.toAbsolutePath() + ", copy it over the committed file (or rerun with "
                    + "-Dopenapi.update=true) and bump the chart version.");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void specCoversTheWholeContract() throws Exception {
        Map<String, Object> spec = generated();
        assertTrue(String.valueOf(spec.get("openapi")).startsWith("3.1"));
        Map<String, Object> paths = (Map<String, Object>) spec.get("paths");
        assertEquals(List.of("/v1/files/{path}"), List.copyOf(paths.keySet()), "exactly one data endpoint, no actuator paths");

        Map<String, Object> op = (Map<String, Object>) ((Map<String, Object>) paths.get("/v1/files/{path}")).get("get");
        Map<String, Object> responses = (Map<String, Object>) op.get("responses");
        for (ErrorCode c : ErrorCode.values()) {
            assertTrue(responses.containsKey(String.valueOf(c.status().value())), "status documented for " + c.code());
        }
        Map<String, Object> ok = (Map<String, Object>) responses.get("200");
        assertTrue(((Map<String, Object>) ok.get("headers")).keySet()
                .containsAll(List.of("X-HSM-Format-Version", "X-HSM-File-Id", "X-HSM-Delivery", "Content-Length")));

        Map<String, Object> schemas = (Map<String, Object>) ((Map<String, Object>) spec.get("components")).get("schemas");
        Map<String, Object> errorCode = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>)
                schemas.get("ErrorResponse")).get("properties")).get("error_code");
        assertEquals(Arrays.stream(ErrorCode.values()).map(ErrorCode::code).toList(), errorCode.get("enum"),
                "every error code, from the enum");
    }

    @Test
    void specAndUiAreNotOnTheFilePort_andUiIsOffByDefault() throws Exception {
        assertEquals(404, get(port, "/actuator/openapi").statusCode());
        assertEquals(404, get(port, "/v3/api-docs").statusCode());
        int ui = get(managementPort, "/actuator/swagger-ui").statusCode();
        assertTrue(ui == 404, "Swagger UI must be off unless SWAGGER_UI_ENABLED=true, got " + ui);
    }
}
