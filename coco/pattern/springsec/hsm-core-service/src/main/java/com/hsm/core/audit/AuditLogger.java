package com.hsm.core.audit;

import com.hsm.chassis.audit.AuditLog;
import org.springframework.stereotype.Component;

/**
 * hsm-core-service's audit entry point, ported from app/audit/logger.py's audit_log().
 * Since OBS-2 step 2 it is a thin facade over hsm-chassis-audit's {@link AuditLog}, kept
 * so the service's many call sites stay {@code auditLogger.log("encrypt", "app_id", …)}.
 *
 * <p>Each event goes to every audit sink: the stdout audit.json line (always), the
 * demo-UI ring buffer ({@link RecentEventsBuffer}), and Splunk HEC when enabled. The
 * chassis adds {@code request_id} (this request's correlation id) and {@code service}
 * (the release, from {@code hsm.chassis.audit.service}).
 *
 * <p>kv alternates key, value. Never include plaintext, DEK material, tokens or
 * exception messages in kv -- hard contract, not enforced in code.
 */
@Component
public class AuditLogger {

    private final AuditLog auditLog;

    public AuditLogger(AuditLog auditLog) {
        this.auditLog = auditLog;
    }

    public void log(String eventType, Object... kv) {
        auditLog.log(eventType, kv);
    }
}
