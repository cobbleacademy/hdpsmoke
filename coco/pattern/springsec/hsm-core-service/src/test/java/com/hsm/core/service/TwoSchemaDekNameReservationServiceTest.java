package com.hsm.core.service;

import com.hsm.core.TwoSchemas;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** {@link DekNameReservationServiceTest} re-run under the production two-schema layout ({@code TwoSchemas}). */
class TwoSchemaDekNameReservationServiceTest extends DekNameReservationServiceTest {
    @DynamicPropertySource
    static void twoSchemas(DynamicPropertyRegistry registry) {
        TwoSchemas.register(registry);
    }
}
