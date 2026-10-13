package com.hsm.chassis.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The one audit API every service on the chassis uses. Adds the common context to each
 * event and hands it to every {@link AuditSink}:
 * <ul>
 *   <li>{@code request_id} -- the current request's id, read from the MDC key the
 *       service's request-id filter fills ({@code hsm.chassis.web.request-id.mdc-key});
 *       omitted outside a request (scheduled jobs, startup);</li>
 *   <li>{@code service} -- which service/release emitted it
 *       ({@code hsm.chassis.audit.service}, default {@code spring.application.name}).</li>
 * </ul>
 * Then the event's own fields, in the order given. An event field with the same name as
 * a context field replaces its value (keeping its position).
 *
 * <p>Never put plaintext, DEKs, tokens or exception messages that may carry secrets in an
 * event -- exception <em>types</em> only. Not enforced in code: a hard rule for callers.
 *
 * <p>Never throws: an audit failure must not fail the business operation. A sink that
 * throws is logged and skipped; the other sinks still receive the event.
 */
public class AuditLog {

    private static final Logger log = LoggerFactory.getLogger(AuditLog.class);

    private final List<AuditSink> sinks;
    private final String service;
    private final String requestIdMdcKey;

    public AuditLog(List<AuditSink> sinks, String service, String requestIdMdcKey) {
        this.sinks = List.copyOf(sinks);
        this.service = service;
        this.requestIdMdcKey = requestIdMdcKey;
    }

    /** {@code kv} alternates key, value: {@code log("encrypt", "app_id", appId, "status", "success")}. */
    public void log(String eventType, Object... kv) {
        Map<String, Object> fields = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            fields.put(String.valueOf(kv[i]), kv[i + 1]);
        }
        log(eventType, fields);
    }

    public void log(String eventType, Map<String, ?> eventFields) {
        Map<String, Object> fields = new LinkedHashMap<>();
        String requestId = requestIdMdcKey == null ? null : MDC.get(requestIdMdcKey);
        if (requestId != null && !requestId.isBlank()) {
            fields.put("request_id", requestId);
        }
        fields.put("service", service);
        fields.putAll(eventFields);

        AuditRecord record = new AuditRecord(eventType, Instant.now(), fields);
        for (AuditSink sink : sinks) {
            try {
                sink.accept(record);
            } catch (RuntimeException e) {
                log.warn("audit_sink_failed sink={} event_type={} error_type={}",
                        sink.getClass().getSimpleName(), eventType, e.getClass().getSimpleName());
            }
        }
    }

    public String service() {
        return service;
    }
}
