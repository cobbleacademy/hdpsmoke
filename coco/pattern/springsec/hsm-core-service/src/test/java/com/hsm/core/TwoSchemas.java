package com.hsm.core;

import org.springframework.test.context.DynamicPropertyRegistry;

/**
 * Production-shaped schema layout for tests: edek_records and kek_registry in
 * {@value #CRYPTO}, registrations and grants in {@value #ACCESS}, exactly as the
 * Flyway migrations place them when CRYPTO_SCHEMA/ACCESS_SCHEMA differ. Every
 * TwoSchema* test re-runs an existing suite under this layout, so a query that
 * only works when both schemas are "public" fails there.
 *
 * <p>One H2-only concession: Flyway's default schema is set to the crypto schema.
 * V14 drops an index by its bare name; Postgres resolves that through the
 * search_path the app's connection-init-sql sets (crypto schema included), but
 * H2 only looks in the current schema. Real-Postgres behaviour, with production
 * Flyway settings, is TwoSchemaPostgresContainerTest's job.
 */
public final class TwoSchemas {

    public static final String CRYPTO = "hsm_crypto";
    public static final String ACCESS = "hsm_access";

    private TwoSchemas() {
    }

    public static void register(DynamicPropertyRegistry registry) {
        registry.add("CRYPTO_SCHEMA", () -> CRYPTO);
        registry.add("ACCESS_SCHEMA", () -> ACCESS);
        registry.add("spring.flyway.default-schema", () -> CRYPTO);
    }
}
