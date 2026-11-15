package com.hsm.core;

import com.hsm.chassis.lock.JobLock;
import com.hsm.core.demo.DevStatusSeedInitializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * hsm-core against real Postgres with the production schema layout and the
 * production Flyway settings (history in ACCESS_SCHEMA, no default-schema override):
 * migrations V1..V15 run (V14 drops an index by its bare name, which only resolves
 * through the search_path the app's connection-init-sql sets), and the app's own
 * queries find tables in both schemas -- registration and grants, encrypt/decrypt,
 * kek-registry, rotate-kek, rekey and revert, EDEK metadata.
 *
 * <p>The connecting role is a plain, freshly created login whose own search_path is
 * the Postgres default, as in a new namespace -- the app must not rely on a DBA
 * having set it. Demo profile for the mock KEK and demo tokens, with only the
 * production migrations (the demo-only ones are H2 SQL) and the demo status seeder
 * stubbed out (its table is demo-only). Skipped when no Docker daemon is available.
 */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("demo")
class TwoSchemaPostgresContainerTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine")
            .withInitScript("twoschema-postgres-init.sql");

    @DynamicPropertySource
    static void postgres(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", () -> "hsm_app");
        registry.add("spring.datasource.password", () -> "hsm_app_pw");
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("spring.flyway.locations", () -> "classpath:db/migration");
        registry.add("spring.flyway.schemas", () -> TwoSchemas.ACCESS);
        registry.add("CRYPTO_SCHEMA", () -> TwoSchemas.CRYPTO);
        registry.add("ACCESS_SCHEMA", () -> TwoSchemas.ACCESS);
        // The base profile's Postgres spelling, not the demo profile's H2 one.
        registry.add("spring.datasource.hikari.connection-init-sql",
                () -> "SET search_path TO " + TwoSchemas.CRYPTO + ", " + TwoSchemas.ACCESS);
    }

    @MockitoBean
    private DevStatusSeedInitializer devStatusSeedInitializer;

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private JobLock jobLock;

    private static final String V1 = "/api/sensec/hsm/v1";

    private static HttpHeaders headers(String token, String appId) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("Authorization", "Bearer " + token);
        h.set("X-App-ID", appId);
        h.set("X-Response-Detail", "full");
        return h;
    }

    private static HttpHeaders app() {
        return headers("demo-token-payments-svc", "payments-svc");
    }

    private static HttpHeaders admin() {
        return headers("demo-token-ops-admin", "ops-admin");
    }

    @Test
    void migrationsSucceedAndTablesAreInTheirSchemas() {
        List<Map<String, Object>> failed = jdbc.queryForList(
                "SELECT version FROM " + TwoSchemas.ACCESS + ".flyway_schema_history WHERE NOT success");
        assertTrue(failed.isEmpty(), "failed migrations: " + failed);
        assertEquals("15", jdbc.queryForObject(
                "SELECT max(version::int)::text FROM " + TwoSchemas.ACCESS + ".flyway_schema_history WHERE version IS NOT NULL",
                String.class));
        assertEquals(TwoSchemas.CRYPTO, jdbc.queryForObject(
                "SELECT schemaname FROM pg_indexes WHERE indexname = 'idx_edek_current_name'", String.class));
        Map.of("edek_records", TwoSchemas.CRYPTO, "kek_registry", TwoSchemas.CRYPTO,
                "app_registrations", TwoSchemas.ACCESS, "app_grants", TwoSchemas.ACCESS,
                "app_dek_grants", TwoSchemas.ACCESS, "app_classification_grants", TwoSchemas.ACCESS)
                .forEach((table, schema) -> assertEquals(List.of(schema), jdbc.queryForList(
                        "SELECT table_schema FROM information_schema.tables WHERE table_name = ?", String.class, table), table));
    }

    @Test
    void encryptDecryptKekRegistryRotateRekeyAndRevertWorkAcrossTwoSchemas() {
        String kekA = "pg-two-schema-kek-a";
        String kekB = "pg-two-schema-kek-b";
        String dekName = "pg-two-schema-dek";

        assertEquals(HttpStatus.CREATED, rest.postForEntity(V1 + "/admin/kek-registry", new HttpEntity<>(
                Map.of("app_id", "payments-svc", "dek_name", dekName, "kek_name", kekA), admin()), Map.class).getStatusCode());

        ResponseEntity<Map> enc = rest.postForEntity(V1 + "/encrypt", new HttpEntity<>(
                Map.of("plaintext", "two schemas", "dek_name", dekName), app()), Map.class);
        assertEquals(HttpStatus.CREATED, enc.getStatusCode());
        String token = (String) enc.getBody().get("ciphertext");
        String edekId = (String) enc.getBody().get("edek_id");

        ResponseEntity<Map> rotate = rest.postForEntity(V1 + "/admin/rotate-kek?kekName=" + kekA,
                new HttpEntity<>(admin()), Map.class);
        assertEquals(HttpStatus.OK, rotate.getStatusCode());

        ResponseEntity<Map> rekey = rest.postForEntity(V1 + "/admin/rekey-kek", new HttpEntity<>(
                Map.of("from_kek_name", kekA, "to_kek_name", kekB), admin()), Map.class);
        assertEquals(HttpStatus.OK, rekey.getStatusCode());
        assertEquals(kekB, metadata(edekId).get("kek_name"));
        assertEquals("two schemas", decrypt(token));

        ResponseEntity<Map> revert = rest.postForEntity(V1 + "/admin/rekey-kek/revert", new HttpEntity<>(
                Map.of("kek_name", kekB), admin()), Map.class);
        assertEquals(HttpStatus.OK, revert.getStatusCode());
        assertEquals(kekA, metadata(edekId).get("kek_name"));
        assertEquals("two schemas", decrypt(token));

        ResponseEntity<Map> registry = rest.exchange(V1 + "/admin/kek-registry", HttpMethod.GET,
                new HttpEntity<>(admin()), Map.class);
        assertEquals(HttpStatus.OK, registry.getStatusCode());
    }

    @Test
    void grantsAreStoredAndListedFromTheAccessSchema() {
        assertEquals(HttpStatus.CREATED, rest.postForEntity(V1 + "/admin/grants", new HttpEntity<>(
                Map.of("grantee_app_id", "ops-admin", "owner_app_id", "pg-reporting-app", "scope", "decrypt"), admin()),
                Map.class).getStatusCode());
        ResponseEntity<Map> list = rest.exchange(V1 + "/admin/grants", HttpMethod.GET, new HttpEntity<>(admin()), Map.class);
        assertEquals(HttpStatus.OK, list.getStatusCode());
        assertTrue(String.valueOf(list.getBody()).contains("pg-reporting-app"));
    }

    @Test
    void anotherEnvironmentsRotationLockOnTheSameDatabaseDoesNotBlockThisOne() throws Exception {
        // Advisory locks are database-wide: hold the lock another environment (own
        // crypto schema) would take, through a real Postgres session, while this one rotates.
        CountDownLatch acquired = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread otherEnvironment = new Thread(() -> jobLock.tryRun("hsm:kek-wrap:hsm_crypto_other_env", () -> {
            acquired.countDown();
            try {
                release.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return true;
        }));
        otherEnvironment.start();
        assertTrue(acquired.await(10, TimeUnit.SECONDS));
        try {
            assertEquals(1, jdbc.queryForObject(
                    "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted", Integer.class));
            ResponseEntity<Map> rotate = rest.postForEntity(V1 + "/admin/rotate-kek", new HttpEntity<>(admin()), Map.class);
            assertEquals(HttpStatus.OK, rotate.getStatusCode());
        } finally {
            release.countDown();
            otherEnvironment.join(20_000);
        }
    }

    private Map metadata(String edekId) {
        return rest.exchange(V1 + "/admin/edek/" + edekId, HttpMethod.GET, new HttpEntity<>(admin()), Map.class).getBody();
    }

    private String decrypt(String token) {
        ResponseEntity<Map> resp = rest.postForEntity(V1 + "/decrypt", new HttpEntity<>(Map.of("ciphertext", token), app()), Map.class);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        return (String) resp.getBody().get("plaintext");
    }
}
