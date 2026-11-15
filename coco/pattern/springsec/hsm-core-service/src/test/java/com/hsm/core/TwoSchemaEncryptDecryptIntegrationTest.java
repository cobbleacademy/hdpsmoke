package com.hsm.core;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** {@link EncryptDecryptIntegrationTest} (encrypt/decrypt, grants, kek-registry, rotate-kek, EDEK metadata) under {@link TwoSchemas}. */
class TwoSchemaEncryptDecryptIntegrationTest extends EncryptDecryptIntegrationTest {
    @DynamicPropertySource
    static void twoSchemas(DynamicPropertyRegistry registry) {
        TwoSchemas.register(registry);
    }
}
