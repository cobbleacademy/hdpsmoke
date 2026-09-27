package com.hsm.fileservice.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * One JSON line per file request on the {@code audit.json} logger -- the same stream
 * convention as hsm-core-service (see logback-spring.xml), so the same Splunk/Log
 * Analytics pipeline picks it up.
 *
 * <p>{@code end_user} is whatever the BFF put in X-End-User: recorded for the audit
 * trail, never used to allow or deny (under the option-A trust model the BFF is the
 * authority on users). {@code caller} is the mesh-authenticated identity that actually
 * made the call.
 */
@Component
public class AccessAuditLogger {

    private static final Logger AUDIT = LoggerFactory.getLogger("audit.json");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public record Event(String requestId, String outcome, String errorCode, String mode, String path,
                        String endUser, String caller, String fileId, String formatVersion,
                        long bytes, long durationMs) {
    }

    public void log(Event e) {
        ObjectNode line = MAPPER.createObjectNode()
                .put("event", "file_access")
                .put("timestamp", Instant.now().toString())
                .put("request_id", e.requestId())
                .put("outcome", e.outcome())
                .put("error_code", e.errorCode())
                .put("mode", e.mode())
                .put("path", e.path())
                .put("end_user", e.endUser())
                .put("caller", e.caller())
                .put("file_id", e.fileId())
                .put("format_version", e.formatVersion())
                .put("bytes", e.bytes())
                .put("duration_ms", e.durationMs());
        try {
            AUDIT.info(MAPPER.writeValueAsString(line));
        } catch (Exception ex) {
            AUDIT.info("{\"event\":\"file_access\",\"audit_serialization_failed\":true}");
        }
    }
}
