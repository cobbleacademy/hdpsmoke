package com.hsm.core;

import com.hsm.chassis.fips.FipsGuard;
import com.hsm.chassis.lock.InProcessJobLock;
import com.hsm.chassis.lock.JobLock;
import com.hsm.crypto.FipsBootstrap;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * hsm-core-service on the chassis keeps its own contracts: the X-Correlation-Id
 * request-id contract (now enforced by hsm-chassis-web's RequestIdFilter), the exact
 * admin error text for an unparseable public key, BC-FIPS approved-only on every
 * thread, and a JobLock from hsm-chassis-core. API shapes are pinned separately by
 * ApiContractBaselineTest.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("demo")
class ChassisAdoptionTest {

    private static final String PREFIX = "/api/sensec/hsm/v1";

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:chassis-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private ApplicationContext context;

    private static HttpHeaders auth(String app) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("Authorization", "Bearer demo-token-" + app);
        h.set("X-App-ID", app);
        return h;
    }

    @SuppressWarnings("rawtypes")
    private ResponseEntity<Map> encryptWith(HttpHeaders headers) {
        return rest.postForEntity(PREFIX + "/encrypt", new HttpEntity<>(Map.of("plaintext", "x"), headers), Map.class);
    }

    @Test
    @SuppressWarnings("rawtypes")
    void callerCorrelationIdIsEchoedAndUsedInTheResponseBody() {
        HttpHeaders h = auth("payments-svc");
        h.set("X-Correlation-Id", "caller-trace-42");

        ResponseEntity<Map> r = encryptWith(h);

        assertEquals(HttpStatus.CREATED, r.getStatusCode());
        assertEquals("caller-trace-42", r.getHeaders().getFirst("X-Correlation-Id"));
        assertEquals("caller-trace-42", r.getBody().get("correlation_id"), "MDC key correlationId still feeds the body");
    }

    @Test
    @SuppressWarnings("rawtypes")
    void requestIdFromAServiceThatOnlySendsXRequestIdIsAdopted() {
        HttpHeaders h = auth("payments-svc");
        h.set("X-Request-Id", "from-file-service-7");

        ResponseEntity<Map> r = encryptWith(h);

        assertEquals("from-file-service-7", r.getHeaders().getFirst("X-Correlation-Id"));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void unsafeCorrelationIdIsReplaced() {
        HttpHeaders h = auth("payments-svc");
        h.set("X-Correlation-Id", "bad id<script>");

        ResponseEntity<Map> r = encryptWith(h);

        String id = r.getHeaders().getFirst("X-Correlation-Id");
        assertNotNull(id);
        assertDoesNotThrow(() -> UUID.fromString(id));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void unauthenticatedResponseStillCarriesACorrelationId() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("X-App-ID", "payments-svc");
        h.set("X-Correlation-Id", "auth-fail-trace");

        ResponseEntity<Map> r = encryptWith(h);

        assertEquals(HttpStatus.UNAUTHORIZED, r.getStatusCode());
        assertEquals("auth-fail-trace", r.getHeaders().getFirst("X-Correlation-Id"));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void invalidPublicKeyDetailTextIsUnchanged() {
        ResponseEntity<Map> r = rest.postForEntity(PREFIX + "/admin/apps/keys",
                new HttpEntity<>(Map.of("app_id", "payments-svc", "encryption_public_key_pem", "not-a-pem"), auth("ops-admin")),
                Map.class);

        assertEquals(HttpStatus.UNPROCESSABLE_CONTENT, r.getStatusCode());
        assertEquals("invalid encryption_public_key_pem: Invalid public_key_pem: Illegal base64 character 2d",
                r.getBody().get("detail"));
    }

    @Test
    void fipsIsApprovedOnlyOnEveryThreadAndChassisBeansArePresent() {
        assertTrue(FipsBootstrap.isApprovedOnlyJvmWide());
        assertEquals(1, context.getBeansOfType(FipsGuard.class).size());
        assertInstanceOf(InProcessJobLock.class, context.getBean(JobLock.class), "H2 in tests -> in-process lock");
    }
}
