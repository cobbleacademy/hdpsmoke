package com.hsm.core;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** {@link InputValidationIntegrationTest} re-run under the production two-schema layout ({@code TwoSchemas}). */
class TwoSchemaInputValidationIntegrationTest extends InputValidationIntegrationTest {
    @DynamicPropertySource
    static void twoSchemas(DynamicPropertyRegistry registry) {
        TwoSchemas.register(registry);
    }
}
