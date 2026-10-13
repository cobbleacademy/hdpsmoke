package com.hsm.core.audit;

import com.hsm.core.service.RotationService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * OBS-2 step 1: every audit event names the emitting release ({@code service}) and,
 * inside a request, carries that request's correlation id as {@code request_id} -- so
 * an audit record joins up with the request's log lines and with the caller's own
 * audit records. Existing fields are unchanged (additive only).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("demo")
class AuditContextTest {

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:auditctx-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private RecentEventsBuffer recentEvents;

    @Autowired
    private RotationService rotationService;

    private Map<String, Object> latest(String eventType) {
        return recentEvents.recent(-1).stream()
                .filter(e -> eventType.equals(e.get("event_type")))
                .findFirst().orElseThrow(() -> new AssertionError("no " + eventType + " event"));
    }

    @Test
    void requestEventCarriesTheRequestsCorrelationIdAndService() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("Authorization", "Bearer demo-token-payments-svc");
        h.set("X-App-ID", "payments-svc");
        h.set("X-Correlation-Id", "audit-trace-1");

        assertEquals(HttpStatus.CREATED, rest.postForEntity("/api/sensec/hsm/v1/encrypt",
                new HttpEntity<>(Map.of("plaintext", "x"), h), Map.class).getStatusCode());

        Map<String, Object> encrypt = latest("encrypt");
        assertEquals("audit-trace-1", encrypt.get("request_id"));
        assertEquals("hsm-core-service", encrypt.get("service"));
        assertEquals("payments-svc", encrypt.get("app_id"), "existing fields unchanged");
        assertEquals("success", encrypt.get("status"));
    }

    @Test
    void eventOutsideARequestHasServiceButNoRequestId() {
        rotationService.rotateNamedDeks(720);

        Map<String, Object> rotation = latest("named_dek_rotation_completed");
        assertEquals("hsm-core-service", rotation.get("service"));
        assertFalse(rotation.containsKey("request_id"), "background jobs aren't part of a request");
    }
}
