package com.hsm.fileservice.audit;

import com.hsm.chassis.audit.AuditLog;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One {@code file_access} audit event per file request, through hsm-chassis-audit's
 * {@link AuditLog} -- the same implementation hsm-core-service uses, so both services'
 * events share the common envelope ({@code event}, {@code event_type}, {@code level},
 * {@code logger}, {@code host}, {@code timestamp}, {@code request_id}, {@code service})
 * on the same audit.json stream. Every field this event had before is kept, with the
 * same name and value format (timestamp stays an ISO-8601 instant).
 *
 * <p>{@code end_user} is whatever the BFF put in X-End-User: recorded for the audit
 * trail, never used to allow or deny (under the option-A trust model the BFF is the
 * authority on users). {@code caller} is the mesh-authenticated identity that actually
 * made the call.
 */
@Component
public class AccessAuditLogger {

    private final AuditLog auditLog;

    public AccessAuditLogger(AuditLog auditLog) {
        this.auditLog = auditLog;
    }

    public record Event(String requestId, String outcome, String errorCode, String mode, String path,
                        String endUser, String caller, String fileId, String formatVersion,
                        long bytes, long durationMs) {
    }

    public void log(Event e) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("request_id", e.requestId());
        fields.put("outcome", e.outcome());
        fields.put("error_code", e.errorCode());
        fields.put("mode", e.mode());
        fields.put("path", e.path());
        fields.put("end_user", e.endUser());
        fields.put("caller", e.caller());
        fields.put("file_id", e.fileId());
        fields.put("format_version", e.formatVersion());
        fields.put("bytes", e.bytes());
        fields.put("duration_ms", e.durationMs());
        auditLog.log("file_access", fields);
    }
}
