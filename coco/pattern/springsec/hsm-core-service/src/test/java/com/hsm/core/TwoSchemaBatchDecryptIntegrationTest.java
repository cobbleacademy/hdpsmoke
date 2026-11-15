package com.hsm.core;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** {@link BatchDecryptIntegrationTest} re-run under the production two-schema layout ({@code TwoSchemas}). */
class TwoSchemaBatchDecryptIntegrationTest extends BatchDecryptIntegrationTest {
    @DynamicPropertySource
    static void twoSchemas(DynamicPropertyRegistry registry) {
        TwoSchemas.register(registry);
    }
}
