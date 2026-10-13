package com.hsm.core.audit;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins hsm-core-service's audit stdout line (the audit.json stream Splunk ingests) to the
 * shape OBS-2 step 1 shipped: field names, field order and value formats. Written before
 * the move to hsm-chassis-audit so the refactor can prove it changed nothing.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("demo")
class AuditLineFormatTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:auditline-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private TestRestTemplate rest;

    private final Logger auditJson = (Logger) LoggerFactory.getLogger("audit.json");
    private final ListAppender<ILoggingEvent> captured = new ListAppender<>();

    @BeforeEach
    void capture() {
        captured.start();
        auditJson.addAppender(captured);
    }

    @AfterEach
    void release() {
        auditJson.detachAppender(captured);
    }

    private JsonNode lastLine(String eventType) throws Exception {
        for (int i = captured.list.size() - 1; i >= 0; i--) {
            JsonNode line = JSON.readTree(captured.list.get(i).getFormattedMessage());
            if (eventType.equals(line.path("event_type").asText())) {
                return line;
            }
        }
        throw new AssertionError("no audit.json line for " + eventType);
    }

    private static List<String> keys(JsonNode line) {
        List<String> keys = new ArrayList<>();
        line.fieldNames().forEachRemaining(keys::add);
        return keys;
    }

    @Test
    void encryptLineKeepsItsExactShape() throws Exception {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("Authorization", "Bearer demo-token-payments-svc");
        h.set("X-App-ID", "payments-svc");
        h.set("X-Correlation-Id", "line-format-1");
        rest.postForEntity("/api/sensec/hsm/v1/encrypt", new HttpEntity<>(Map.of("plaintext", "x"), h), Map.class);

        JsonNode line = lastLine("encrypt");
        assertEquals(EXPECTED_ENCRYPT_KEYS, keys(line));
        assertEquals("encrypt", line.get("event").asText());
        assertEquals("info", line.get("level").asText());
        assertEquals("audit", line.get("logger").asText());
        assertTrue(line.get("timestamp").asText().matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z"),
                "seconds precision, UTC: " + line.get("timestamp").asText());
        assertEquals("line-format-1", line.get("request_id").asText());
        assertEquals("hsm-core-service", line.get("service").asText());
        assertEquals("payments-svc", line.get("app_id").asText());
    }

    /** Captured from the OBS-2 step-1 code (commit 28bf1ec) -- the shape Splunk sees today. */
    private static final List<String> EXPECTED_ENCRYPT_KEYS = List.of(
            "event", "event_type", "level", "logger", "host", "timestamp", "request_id", "service",
            "app_id", "sub", "end_user_id", "edek_id", "kek_version", "data_classification", "caller_ip",
            "context", "dek_name", "reused", "status");
}
