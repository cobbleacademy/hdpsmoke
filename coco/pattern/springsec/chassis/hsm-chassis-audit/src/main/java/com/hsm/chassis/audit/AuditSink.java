package com.hsm.chassis.audit;

/**
 * Where audit events go. Every {@code AuditSink} bean receives every event: the chassis
 * provides {@link StdoutAuditSink} (always) and {@link SplunkHecSink} (when enabled); a
 * service can add its own (hsm-core-service's demo-UI ring buffer is one).
 *
 * <p>Must not throw for ordinary failures and must not block for long -- it runs on the
 * thread that emitted the event. {@link AuditLog} isolates sinks from each other anyway.
 */
public interface AuditSink {

    void accept(AuditRecord record);
}
