package com.hsm.core;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/**
 * Smoke test: the app context boots under the demo profile with H2 +
 * Flyway (V1, V2, and the demo-only V3 consumer-accounts migration) applied
 * cleanly, before any repositories/entities/services are layered on top.
 */
@SpringBootTest
@ActiveProfiles("demo")
class DemoProfileContextLoadTest {

    @Test
    void contextLoads() {
    }
}
