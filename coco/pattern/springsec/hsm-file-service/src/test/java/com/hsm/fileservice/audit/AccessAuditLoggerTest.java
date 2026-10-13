package com.hsm.fileservice.audit;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsm.chassis.audit.AuditLog;
import com.hsm.chassis.audit.StdoutAuditSink;
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

    /** file_access fields as OBS-2 step 1 emitted them (commit 28bf1ec). */
    private static final List<String> STEP1_KEYS = List.of("event", "timestamp", "service", "host", "request_id",
            "outcome", "error_code", "mode", "path", "end_user", "caller", "file_id", "format_version", "bytes", "duration_ms");

    @Test
    void lineCarriesServiceAndHostPlusAllExistingFields() throws Exception {
        AuditLog auditLog = new AuditLog(List.of(new StdoutAuditSink(StdoutAuditSink.TimestampFormat.ISO_INSTANT)),
                "hsm-file-service", "requestId");
        new AccessAuditLogger(auditLog).log(new AccessAuditLogger.Event(
                "req-1", "success", null, "buffered", "tenant-a/report.pdf", "alice@example.com",
                "spiffe://cluster.local/ns/bff/sa/bff", "file-id-1", "2", 123, 4));

        JsonNode line = new ObjectMapper().readTree(captured.list.get(0).getFormattedMessage());
        assertEquals("hsm-file-service", line.get("service").asText());
        assertFalse(line.get("host").asText().isBlank());
        for (String existing : List.of("event", "timestamp", "request_id", "outcome", "error_code", "mode", "path",
                "end_user", "caller", "file_id", "format_version", "bytes", "duration_ms")) {
            assertEquals(true, line.has(existing), "existing field kept: " + existing);
        }
        List<String> keys = new java.util.ArrayList<>();
        line.fieldNames().forEachRemaining(keys::add);
        // Every field the step-1 line had (captured from commit 28bf1ec) must still be there;
        // the only additions allowed are the common audit envelope's (OBS-2 option B).
        java.util.Set<String> extra = new java.util.LinkedHashSet<>(keys);
        STEP1_KEYS.forEach(extra::remove);
        assertEquals(true, keys.containsAll(STEP1_KEYS), "step-1 fields kept: " + keys);
        assertEquals(true, java.util.Set.of("event_type", "level", "logger").containsAll(extra), "unexpected new fields: " + extra);
        assertEquals(true, line.get("timestamp").asText().matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d+Z"),
                "ISO-8601 instant with fractional seconds, as before: " + line.get("timestamp").asText());
        assertEquals("file_access", line.get("event").asText());
        assertEquals("req-1", line.get("request_id").asText());
    }
}
