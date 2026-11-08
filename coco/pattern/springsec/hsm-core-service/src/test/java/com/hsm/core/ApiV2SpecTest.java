package com.hsm.core;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.lang.reflect.RecordComponent;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The v2 OpenAPI document ({@code <api-v2-prefix>/openapi}): camelCase fields that are the
 * DTOs' real field names (what v2 puts on the wire), documented error responses, and a
 * committed copy -- java/docs/oas/hsm-core-service-v2.yaml, the file shared with the
 * platform/Apigee team -- that must match what the service generates. After an intentional
 * API change, review target/generated-openapi-v2.yaml and copy it over the committed file,
 * or run with -Dopenapi.update=true.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("demo")
class ApiV2SpecTest {

    static final Path COMMITTED = Path.of("..", "docs", "oas", "hsm-core-service-v2.yaml").toAbsolutePath().normalize();
    static final Path GENERATED = Path.of("target", "generated-openapi-v2.yaml");

    private static final String V1_SPEC = "/api/sensec/hsm/v1/openapi";
    private static final String V2_SPEC = "/api/sensec/hsm/v2/openapi";

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:hsmoas2-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    TestRestTemplate rest;

    @SuppressWarnings("unchecked")
    private Map<String, Object> fetch(String url) {
        ResponseEntity<Map> resp = rest.getForEntity(url, Map.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode(), url);
        return resp.getBody();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Map<String, Object>> schemas(Map<String, Object> spec) {
        return (Map<String, Map<String, Object>>) ((Map<String, Object>) spec.get("components")).get("schemas");
    }

    @Test
    @SuppressWarnings("unchecked")
    void describesTheV2Contract() {
        Map<String, Object> spec = fetch(V2_SPEC);
        assertTrue(String.valueOf(spec.get("openapi")).startsWith("3.1"));
        assertEquals("2.0.0", ((Map<String, Object>) spec.get("info")).get("version"));
        assertEquals(List.of(Map.of("url", ".", "description", "The API prefix this document was fetched from")), spec.get("servers"));

        Map<String, Object> paths = (Map<String, Object>) spec.get("paths");
        assertTrue(paths.containsKey("/encrypt") && paths.containsKey("/dek/unwrap") && paths.containsKey("/admin/grants"));
        assertFalse(paths.keySet().stream().anyMatch(p -> p.startsWith("/api/")), "paths relative to the v2 prefix");
        assertFalse(paths.keySet().stream().anyMatch(p -> p.contains("/demo/")));

        Map<String, Object> encryptProps = (Map<String, Object>) schemas(spec).get("EncryptRequest").get("properties");
        assertTrue(encryptProps.containsKey("dataClassification"));
        assertFalse(encryptProps.containsKey("data_classification"));
        assertEquals(HttpStatus.OK, rest.getForEntity(V2_SPEC + ".yaml", String.class).getStatusCode(), "YAML variant");
    }

    /** Every documented field must be one v2 actually reads/writes: the DTO's record component. */
    @Test
    void fieldNamesAreTheDtoFieldNames() throws Exception {
        int checked = 0;
        for (Map.Entry<String, Map<String, Object>> schema : schemas(fetch(V2_SPEC)).entrySet()) {
            Class<?> dto;
            try {
                dto = Class.forName("com.hsm.core.dto." + schema.getKey());
            } catch (ClassNotFoundException notADto) {
                continue; // e.g. ErrorResponse, documented by OpenApiConfig itself
            }
            assertTrue(dto.isRecord(), schema.getKey());
            Set<String> javaFields = Arrays.stream(dto.getRecordComponents()).map(RecordComponent::getName).collect(Collectors.toSet());
            @SuppressWarnings("unchecked")
            Map<String, Object> props = (Map<String, Object>) schema.getValue().getOrDefault("properties", Map.of());
            for (String field : props.keySet()) {
                assertTrue(javaFields.contains(field), schema.getKey() + "." + field + " is not a field of " + dto.getSimpleName());
                checked++;
            }
        }
        assertTrue(checked > 100, "checked " + checked + " fields");
    }

    @Test
    void v1DocumentIsUnaffectedByV2() {
        // springdoc may share schema objects between documents; renaming v2's must not leak into v1.
        Map<String, Object> before = fetch(V1_SPEC);
        fetch(V2_SPEC);
        Map<String, Object> after = fetch(V1_SPEC);
        assertEquals(before, after);
        assertTrue(schemas(after).get("EncryptRequest").toString().contains("data_classification"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void documentsErrorResponsesAndBatchContract() {
        for (String url : List.of(V1_SPEC, V2_SPEC)) {
            Map<String, Object> paths = (Map<String, Object>) fetch(url).get("paths");
            Map<String, Object> encrypt = (Map<String, Object>) ((Map<String, Object>) paths.get("/encrypt")).get("post");
            assertEquals(Set.of("201", "400", "401", "403", "422", "500"), ((Map<String, Object>) encrypt.get("responses")).keySet(), url);
            Map<String, Object> decrypt = (Map<String, Object>) ((Map<String, Object>) paths.get("/decrypt")).get("post");
            assertTrue(((Map<String, Object>) decrypt.get("responses")).containsKey("404"), url);
            Map<String, Object> health = (Map<String, Object>) ((Map<String, Object>) paths.get("/admin/health")).get("get");
            assertEquals(Set.of("200", "500"), ((Map<String, Object>) health.get("responses")).keySet(), url + ": public, no body");
            Map<String, Object> batch = (Map<String, Object>) ((Map<String, Object>) paths.get("/encrypt/batch")).get("post");
            assertTrue(String.valueOf(batch.get("description")).contains("match results to items"), url);
        }
    }

    @Test
    void committedSpecMatchesWhatTheServiceGenerates() throws Exception {
        DumperOptions opts = new DumperOptions();
        opts.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        opts.setWidth(120);
        String yaml = "# GENERATED from hsm-core-service by ApiV2SpecTest -- do not edit by hand.\n"
                + "# The v2 (camelCase) contract, served at <api-v2-prefix>/openapi.\n"
                + new Yaml(opts).dump(fetch(V2_SPEC));
        Files.createDirectories(GENERATED.getParent());
        Files.writeString(GENERATED, yaml, StandardCharsets.UTF_8);
        if (Boolean.getBoolean("openapi.update")) {
            Files.createDirectories(COMMITTED.getParent());
            Files.writeString(COMMITTED, yaml, StandardCharsets.UTF_8);
            return;
        }
        if (!Files.exists(COMMITTED) || !Files.readString(COMMITTED).equals(yaml)) {
            fail("java/docs/oas/hsm-core-service-v2.yaml differs from the generated v2 spec. Review "
                    + GENERATED.toAbsolutePath() + " and copy it over the committed file (or run with -Dopenapi.update=true).");
        }
    }

    /** Swagger UI (hsm-chassis-web ApiV2Docs): one page with a v1/v2 dropdown, and <v2>/docs opening it on v2. */
    @Test
    @SuppressWarnings("unchecked")
    void swaggerUiOffersBothVersions() throws Exception {
        Map<String, Object> cfg = rest.getForObject("/api/sensec/hsm/v1/openapi/swagger-config", Map.class);
        assertEquals(List.of(Map.of("name", "v1", "url", "../openapi"), Map.of("name", "v2", "url", "../../v2/openapi")),
                cfg.get("urls"));
        assertEquals("v1", cfg.get("urls.primaryName"));

        java.net.http.HttpResponse<Void> docs = java.net.http.HttpClient.newHttpClient().send(
                java.net.http.HttpRequest.newBuilder(java.net.URI.create(rest.getRootUri() + "/api/sensec/hsm/v2/docs")).build(),
                java.net.http.HttpResponse.BodyHandlers.discarding());
        assertEquals(302, docs.statusCode());
        assertEquals("../v1/swagger-ui/index.html?urls.primaryName=v2", docs.headers().firstValue("Location").orElse(null));
    }
}
