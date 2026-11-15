package com.hsm.core;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** {@link ApiV2IntegrationTest} re-run under the production two-schema layout ({@code TwoSchemas}). */
class TwoSchemaApiV2IntegrationTest extends ApiV2IntegrationTest {
    @DynamicPropertySource
    static void twoSchemas(DynamicPropertyRegistry registry) {
        TwoSchemas.register(registry);
    }
}
