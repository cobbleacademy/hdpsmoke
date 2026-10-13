package com.hsm.fileservice.audit;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * OBS-2 step 1: each file_access line names the emitting service and pod itself, in
 * addition to every field it already had (additive only).
 */
class AccessAuditLoggerTest {

    private final Logger auditLogger = (Logger) LoggerFactory.getLogger("audit.json");
    private final ListAppender<ILoggingEvent> captured = new ListAppender<>();

    @BeforeEach
    void capture() {
        captured.start();
        auditLogger.addAppender(captured);
    }

    @AfterEach
    void release() {
        auditLogger.detachAppender(captured);
    }

    @Test
    void lineCarriesServiceAndHostPlusAllExistingFields() throws Exception {
        new AccessAuditLogger("hsm-file-service").log(new AccessAuditLogger.Event(
                "req-1", "success", null, "buffered", "tenant-a/report.pdf", "alice@example.com",
                "spiffe://cluster.local/ns/bff/sa/bff", "file-id-1", "2", 123, 4));

        JsonNode line = new ObjectMapper().readTree(captured.list.get(0).getFormattedMessage());
        assertEquals("hsm-file-service", line.get("service").asText());
        assertFalse(line.get("host").asText().isBlank());
        for (String existing : List.of("event", "timestamp", "request_id", "outcome", "error_code", "mode", "path",
                "end_user", "caller", "file_id", "format_version", "bytes", "duration_ms")) {
            assertEquals(true, line.has(existing), "existing field kept: " + existing);
        }
        assertEquals("file_access", line.get("event").asText());
        assertEquals("req-1", line.get("request_id").asText());
    }
}
