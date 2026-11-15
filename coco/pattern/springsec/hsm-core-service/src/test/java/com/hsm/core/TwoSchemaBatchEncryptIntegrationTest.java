package com.hsm.core;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** {@link BatchEncryptIntegrationTest} re-run under the production two-schema layout ({@code TwoSchemas}). */
class TwoSchemaBatchEncryptIntegrationTest extends BatchEncryptIntegrationTest {
    @DynamicPropertySource
    static void twoSchemas(DynamicPropertyRegistry registry) {
        TwoSchemas.register(registry);
    }
}
