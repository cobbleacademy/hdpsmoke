package com.hsm.core.web;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class V1DeprecationHeadersTest {

    private static void isolatedDb(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:hsmdep-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Nested
    @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
            "hsm.service.api-v1-deprecated-at=2027-01-01", "hsm.service.api-v1-sunset-at=2027-06-30"})
    @AutoConfigureTestRestTemplate
    @ActiveProfiles("demo")
    class WhenDatesAreSet {

        @DynamicPropertySource
        static void props(DynamicPropertyRegistry registry) {
            isolatedDb(registry);
        }

        @Autowired
        TestRestTemplate rest;

        @Test
        void v1ResponsesAnnounceRetirement() {
            HttpHeaders h = rest.getForEntity("/api/sensec/hsm/v1/admin/health", String.class).getHeaders();
            assertEquals("@1798761600", h.getFirst("Deprecation"));
            assertEquals("Wed, 30 Jun 2027 00:00:00 GMT", h.getFirst("Sunset"));
            assertEquals("</api/sensec/hsm/v2>; rel=\"successor-version\"", h.getFirst("Link"));
        }

        @Test
        void evenOnAuthFailures() {
            HttpHeaders h = rest.postForEntity("/api/sensec/hsm/v1/encrypt", null, String.class).getHeaders();
            assertEquals("@1798761600", h.getFirst("Deprecation"));
        }

        @Test
        void v2ResponsesDoNot() {
            HttpHeaders h = rest.getForEntity("/api/sensec/hsm/v2/admin/health", String.class).getHeaders();
            assertNull(h.getFirst("Deprecation"));
            assertNull(h.getFirst("Sunset"));
        }
    }

    @Nested
    @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
    @AutoConfigureTestRestTemplate
    @ActiveProfiles("demo")
    class ByDefault {

        @DynamicPropertySource
        static void props(DynamicPropertyRegistry registry) {
            isolatedDb(registry);
        }

        @Autowired
        TestRestTemplate rest;

        @Test
        void v1ResponsesAreUnchanged() {
            HttpHeaders h = rest.getForEntity("/api/sensec/hsm/v1/admin/health", String.class).getHeaders();
            assertNull(h.getFirst("Deprecation"));
            assertNull(h.getFirst("Sunset"));
        }
    }
}
