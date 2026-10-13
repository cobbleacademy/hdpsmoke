package com.hsm.core.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsm.core.config.HsmProperties;
import com.hsm.core.web.CorrelationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Single entry point for audit events, ported from app/audit/logger.py's audit_log().
 * Each call: (1) writes one JSON line to stdout, (2) appends to the in-memory
 * ring buffer for the demo UI, (3) enqueues to the Splunk HEC batcher (no-op if disabled).
 */
@Component
public class AuditLogger {

    private static final Logger JSON_LOG = LoggerFactory.getLogger("audit.json");
    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);
    private static final String HOST = resolveHostname();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RecentEventsBuffer recentEvents;
    private final SplunkHecBatcher splunkHecBatcher;
    /** Which release emitted the event (hsm-core-service / hsm-bulk-service / a -next release). */
    private final String service;

    public AuditLogger(RecentEventsBuffer recentEvents, SplunkHecBatcher splunkHecBatcher, HsmProperties properties) {
        this.recentEvents = recentEvents;
        this.splunkHecBatcher = splunkHecBatcher;
        // The per-release Splunk source name (chart config.splunkSource) -- set for every
        // release even with Splunk HEC disabled, so it tells core and bulk apart on stdout.
        this.service = properties.splunk().source();
    }

    /**
     * kv alternates key, value pairs, e.g. log("encrypt", "app_id", appId, "status", "success").
     * Never include plaintext or DEK material in kv -- hard contract, not enforced in code.
     */
    public void log(String eventType, Object... kv) {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("event_type", eventType);
        double epochSeconds = Instant.now().toEpochMilli() / 1000.0;
        record.put("_epoch", epochSeconds);
        // Context added to every event (OBS-2 step 1; additive -- no existing field changes):
        // the request's correlation id, so an audit record joins up with the request's log
        // lines and with the caller's own records (hsm-file-service forwards its
        // request_id as X-Correlation-Id); absent outside a request (rotation jobs).
        String requestId = MDC.get(CorrelationId.MDC_KEY);
        if (requestId != null && !requestId.isBlank()) {
            record.put("request_id", requestId);
        }
        record.put("service", service);
        for (int i = 0; i + 1 < kv.length; i += 2) {
            record.put(String.valueOf(kv[i]), kv[i + 1]);
        }

        writeStdoutLine(eventType, epochSeconds, record);

        recentEvents.add(new LinkedHashMap<>(record));
        splunkHecBatcher.enqueue(new LinkedHashMap<>(record));
    }

    private void writeStdoutLine(String eventType, double epochSeconds, Map<String, Object> record) {
        Map<String, Object> stdoutRecord = new LinkedHashMap<>();
        stdoutRecord.put("event", eventType);      // structlog's message-field convention
        stdoutRecord.put("event_type", eventType); // also carried in the original record's kwargs merge
        stdoutRecord.put("level", "info");
        stdoutRecord.put("logger", "audit");
        stdoutRecord.put("host", HOST);
        stdoutRecord.put("timestamp", TIMESTAMP_FORMAT.format(Instant.ofEpochMilli((long) (epochSeconds * 1000))));
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            if (!entry.getKey().equals("_epoch") && !entry.getKey().equals("event_type")) {
                stdoutRecord.put(entry.getKey(), entry.getValue());
            }
        }

        try {
            JSON_LOG.info(MAPPER.writeValueAsString(stdoutRecord));
        } catch (Exception e) {
            JSON_LOG.info("{\"event_type\":\"" + eventType + "\",\"audit_serialization_error\":\"" + e.getMessage() + "\"}");
        }
    }

    private static String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
