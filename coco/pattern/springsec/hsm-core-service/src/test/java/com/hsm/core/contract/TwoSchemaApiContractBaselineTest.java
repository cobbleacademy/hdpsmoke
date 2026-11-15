package com.hsm.core.contract;

import com.hsm.core.TwoSchemas;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** {@link ApiContractBaselineTest} re-run under the production two-schema layout ({@code TwoSchemas}). */
class TwoSchemaApiContractBaselineTest extends ApiContractBaselineTest {
    @DynamicPropertySource
    static void twoSchemas(DynamicPropertyRegistry registry) {
        TwoSchemas.register(registry);
    }
}
