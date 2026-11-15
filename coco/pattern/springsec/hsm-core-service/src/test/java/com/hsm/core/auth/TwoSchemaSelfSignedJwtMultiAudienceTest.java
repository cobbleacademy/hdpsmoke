package com.hsm.core.auth;

import com.hsm.core.TwoSchemas;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** {@link SelfSignedJwtMultiAudienceTest} re-run under the production two-schema layout ({@code TwoSchemas}). */
class TwoSchemaSelfSignedJwtMultiAudienceTest extends SelfSignedJwtMultiAudienceTest {
    @DynamicPropertySource
    static void twoSchemas(DynamicPropertyRegistry registry) {
        TwoSchemas.register(registry);
    }
}
