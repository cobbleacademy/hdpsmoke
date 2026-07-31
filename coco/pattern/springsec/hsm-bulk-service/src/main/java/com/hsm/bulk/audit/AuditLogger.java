package com.hsm.bulk.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Simplified from com.hsm.core.audit.AuditLogger -- same JSON-line-to-stdout shape
 * (so both services' audit trails are structurally identical and could feed the
 * same log pipeline), but no RecentEventsBuffer (feeds hsm-core-service's demo UI
 * live-audit panel, which this module has no equivalent of) and no
 * SplunkHecBatcher (out of scope for this PoC per the plan's confirmed audit-gap
 * decision: only dek_issued/dek_unwrapped summary events are emitted here at all).
 */
@Component
public class AuditLogger {

    private static final Logger JSON_LOG = LoggerFactory.getLogger("audit.json");
    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);
    private static final String HOST = resolveHostname();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public void log(String eventType, Object... kv) {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("event", eventType);
        record.put("event_type", eventType);
        record.put("level", "info");
        record.put("logger", "audit");
        record.put("host", HOST);
        record.put("service", "hsm-bulk-service");
        record.put("timestamp", TIMESTAMP_FORMAT.format(Instant.now()));
        for (int i = 0; i + 1 < kv.length; i += 2) {
            record.put(String.valueOf(kv[i]), kv[i + 1]);
        }

        try {
            JSON_LOG.info(MAPPER.writeValueAsString(record));
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
