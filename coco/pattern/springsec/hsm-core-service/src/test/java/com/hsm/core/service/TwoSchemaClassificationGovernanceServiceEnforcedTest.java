package com.hsm.core.service;

import com.hsm.core.TwoSchemas;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** {@link ClassificationGovernanceServiceEnforcedTest} re-run under the production two-schema layout ({@code TwoSchemas}). */
class TwoSchemaClassificationGovernanceServiceEnforcedTest extends ClassificationGovernanceServiceEnforcedTest {
    @DynamicPropertySource
    static void twoSchemas(DynamicPropertyRegistry registry) {
        TwoSchemas.register(registry);
    }
}
