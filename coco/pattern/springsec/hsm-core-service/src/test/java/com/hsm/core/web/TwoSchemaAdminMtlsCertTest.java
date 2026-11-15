package com.hsm.core.web;

import com.hsm.core.TwoSchemas;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** {@link AdminMtlsCertTest} re-run under the production two-schema layout ({@code TwoSchemas}). */
class TwoSchemaAdminMtlsCertTest extends AdminMtlsCertTest {
    @DynamicPropertySource
    static void twoSchemas(DynamicPropertyRegistry registry) {
        TwoSchemas.register(registry);
    }
}
