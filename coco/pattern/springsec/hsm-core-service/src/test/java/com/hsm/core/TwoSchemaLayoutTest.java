package com.hsm.core;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Under {@link TwoSchemas}, each table lives in the schema the migrations promise and
 * nowhere else -- so the TwoSchema* suites passing really means the app found them
 * across two schemas, not a stray copy in one.
 */
@SpringBootTest
@ActiveProfiles("demo")
class TwoSchemaLayoutTest {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:twoschema-layout-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        TwoSchemas.register(registry);
    }

    private static final Map<String, String> EXPECTED = Map.of(
            "edek_records", TwoSchemas.CRYPTO,
            "kek_registry", TwoSchemas.CRYPTO,
            "app_registrations", TwoSchemas.ACCESS,
            "app_grants", TwoSchemas.ACCESS,
            "app_dek_grants", TwoSchemas.ACCESS,
            "app_classification_grants", TwoSchemas.ACCESS);

    @Autowired
    private JdbcTemplate jdbc;

    @Test
    void everyTableIsInItsOwnSchemaOnly() {
        EXPECTED.forEach((table, schema) -> {
            List<String> schemas = jdbc.queryForList(
                    "SELECT table_schema FROM information_schema.tables WHERE table_name = ?", String.class, table);
            assertEquals(List.of(schema), schemas, table);
        });
    }
}
