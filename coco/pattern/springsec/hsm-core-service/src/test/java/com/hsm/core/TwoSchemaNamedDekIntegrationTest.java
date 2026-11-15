package com.hsm.core;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** {@link NamedDekIntegrationTest} re-run under the production two-schema layout ({@code TwoSchemas}). */
class TwoSchemaNamedDekIntegrationTest extends NamedDekIntegrationTest {
    @DynamicPropertySource
    static void twoSchemas(DynamicPropertyRegistry registry) {
        TwoSchemas.register(registry);
    }
}
