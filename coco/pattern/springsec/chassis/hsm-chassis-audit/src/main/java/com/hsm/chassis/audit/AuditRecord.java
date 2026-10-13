package com.hsm.chassis.audit;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One audit event as handed to every {@link AuditSink}: its type, when it happened, and
 * its fields in emission order -- the common context first ({@code request_id} when the
 * event happened inside a request, then {@code service}), then the event's own fields.
 * Each sink renders it in its own wire format.
 */
public record AuditRecord(String eventType, Instant time, Map<String, Object> fields) {

    public AuditRecord {
        fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }

    /** Epoch seconds with millisecond precision -- the form Splunk HEC's {@code time} and the legacy {@code _epoch} use. */
    public double epochSeconds() {
        return time.toEpochMilli() / 1000.0;
    }
}
