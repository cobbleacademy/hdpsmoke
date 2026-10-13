package com.hsm.chassis.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Writes each event as one JSON line on the {@code audit.json} logger -- the stream every
 * service routes to stdout unchanged ({@code %msg%n} in its logback-spring.xml) and Splunk
 * ingests. This is the complete audit record; HEC delivery is best-effort on top of it.
 *
 * <pre>
 * {"event":T,"event_type":T,"level":"info","logger":"audit","host":H,"timestamp":TS, ...record fields}
 * </pre>
 *
 * {@code timestamp} keeps each service's existing format so no Splunk search changes
 * ({@code hsm.chassis.audit.timestamp-format}): {@link TimestampFormat#SECONDS} for
 * hsm-core-service ({@code 2026-10-12T14:03:07Z}), {@link TimestampFormat#ISO_INSTANT} for
 * hsm-file-service ({@code 2026-10-12T14:03:07.123456789Z}).
 */
public class StdoutAuditSink implements AuditSink {

    public enum TimestampFormat {
        /** {@code yyyy-MM-ddTHH:mm:ssZ}, UTC, whole seconds. */
        SECONDS,
        /** ISO-8601 instant with the clock's full fractional precision. */
        ISO_INSTANT
    }

    private static final Logger AUDIT_JSON = LoggerFactory.getLogger("audit.json");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter SECONDS_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);
    private static final String HOST = resolveHostname();

    private final TimestampFormat timestampFormat;

    public StdoutAuditSink(TimestampFormat timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

    @Override
    public void accept(AuditRecord record) {
        Map<String, Object> line = new LinkedHashMap<>();
        line.put("event", record.eventType());
        line.put("event_type", record.eventType());
        line.put("level", "info");
        line.put("logger", "audit");
        line.put("host", HOST);
        line.put("timestamp", timestampFormat == TimestampFormat.SECONDS
                ? SECONDS_FORMAT.format(record.time().truncatedTo(ChronoUnit.MILLIS))
                : record.time().toString());
        line.putAll(record.fields());
        try {
            AUDIT_JSON.info(MAPPER.writeValueAsString(line));
        } catch (Exception e) {
            AUDIT_JSON.info("{\"event\":\"" + record.eventType() + "\",\"event_type\":\"" + record.eventType()
                    + "\",\"audit_serialization_error\":\"" + e.getClass().getSimpleName() + "\"}");
        }
    }

    static String host() {
        return HOST;
    }

    private static String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
