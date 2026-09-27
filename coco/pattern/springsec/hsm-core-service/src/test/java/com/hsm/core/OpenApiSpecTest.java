package com.hsm.core;

import org.junit.jupiter.api.Nested;
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

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The generated OpenAPI spec is served only when enabled (demo profile / SPRINGDOC_ENABLED),
 * and when it is, it describes the real wire format: snake_case fields (swagger-core's
 * own mapper would otherwise emit camelCase), no demo-only endpoints, bearer + X-App-ID
 * required everywhere except /admin/health.
 */
class OpenApiSpecTest {

    private static final String SPEC = "/api/sensec/hsm/v1/openapi";
    private static final String UI = "/api/sensec/hsm/v1/swagger-ui/index.html";

    private static void isolatedDb(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:hsmoas-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Nested
    @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
    @AutoConfigureTestRestTemplate
    @ActiveProfiles("demo")
    class WhenEnabled {

        @DynamicPropertySource
        static void props(DynamicPropertyRegistry registry) {
            isolatedDb(registry);
        }

        @Autowired
        TestRestTemplate rest;

        @Test
        @SuppressWarnings("unchecked")
        void specDescribesTheRealContract() {
            ResponseEntity<Map> resp = rest.getForEntity(SPEC, Map.class);
            assertEquals(HttpStatus.OK, resp.getStatusCode());
            Map<String, Object> spec = resp.getBody();
            assertTrue(String.valueOf(spec.get("openapi")).startsWith("3.1"));

            Map<String, Object> paths = (Map<String, Object>) spec.get("paths");
            assertTrue(paths.containsKey("/api/sensec/hsm/v1/encrypt"));
            assertTrue(paths.containsKey("/api/sensec/hsm/v1/dek/unwrap"));
            assertTrue(paths.containsKey("/api/sensec/hsm/v1/admin/grants"));
            assertFalse(paths.keySet().stream().anyMatch(p -> p.contains("/demo/")), "demo endpoints are not part of the contract");

            Map<String, Object> schemas = (Map<String, Object>) ((Map<String, Object>) spec.get("components")).get("schemas");
            Map<String, Object> encryptProps = (Map<String, Object>) ((Map<String, Object>) schemas.get("EncryptRequest")).get("properties");
            assertTrue(encryptProps.containsKey("data_classification"), "snake_case, as on the wire");
            assertTrue(encryptProps.containsKey("dek_name"));
            assertFalse(encryptProps.containsKey("dataClassification"));

            assertEquals(List.of(Map.of("bearerAuth", List.of(), "appId", List.of())), spec.get("security"));
            Map<String, Object> health = (Map<String, Object>) ((Map<String, Object>) paths.get("/api/sensec/hsm/v1/admin/health")).get("get");
            assertEquals(List.of(), health.get("security"), "health is public");
        }

        @Test
        void swaggerUiIsServed() {
            assertEquals(HttpStatus.OK, rest.getForEntity(UI, String.class).getStatusCode());
        }
    }

    @Nested
    @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
            properties = {"springdoc.api-docs.enabled=false", "springdoc.swagger-ui.enabled=false"})
    @AutoConfigureTestRestTemplate
    @ActiveProfiles("demo")
    class WhenDisabled {

        @DynamicPropertySource
        static void props(DynamicPropertyRegistry registry) {
            isolatedDb(registry);
        }

        @Autowired
        TestRestTemplate rest;

        @Test
        void neitherSpecNorUiIsServed() {
            // Same switches as production's default (application.yml: SPRINGDOC_ENABLED:false).
            assertEquals(HttpStatus.NOT_FOUND, rest.getForEntity(SPEC, String.class).getStatusCode());
            assertEquals(HttpStatus.NOT_FOUND, rest.getForEntity(UI, String.class).getStatusCode());
        }
    }
}
