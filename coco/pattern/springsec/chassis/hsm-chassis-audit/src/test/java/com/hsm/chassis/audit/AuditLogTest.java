package com.hsm.chassis.audit;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AuditLogTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    private static List<AuditRecord> capture(List<AuditSink> extra, Runnable body, String service) {
        List<AuditRecord> seen = new ArrayList<>();
        List<AuditSink> sinks = new ArrayList<>(extra);
        sinks.add(seen::add);
        AuditLog log = new AuditLog(sinks, service, "correlationId");
        CURRENT.set(log);
        body.run();
        return seen;
    }

    private static final ThreadLocal<AuditLog> CURRENT = new ThreadLocal<>();

    @Test
    void contextFirstThenEventFieldsInOrder() {
        MDC.put("correlationId", "req-7");
        List<AuditRecord> seen = capture(List.of(),
                () -> CURRENT.get().log("encrypt", "app_id", "payments-svc", "status", "success"), "hsm-core-service");

        AuditRecord r = seen.get(0);
        assertThat(r.eventType()).isEqualTo("encrypt");
        assertThat(new ArrayList<>(r.fields().keySet())).containsExactly("request_id", "service", "app_id", "status");
        assertThat(r.fields()).containsEntry("request_id", "req-7").containsEntry("service", "hsm-core-service");
    }

    @Test
    void noRequestIdOutsideARequest() {
        List<AuditRecord> seen = capture(List.of(), () -> CURRENT.get().log("named_dek_rotation_completed", "status", "success"), "svc");
        assertThat(seen.get(0).fields()).doesNotContainKey("request_id").containsEntry("service", "svc");
    }

    @Test
    void anEventFieldWithAContextNameKeepsItsPositionAndTakesItsValue() {
        MDC.put("correlationId", "from-mdc");
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("request_id", "from-event");
        fields.put("outcome", "success");
        List<AuditRecord> seen = capture(List.of(), () -> CURRENT.get().log("file_access", fields), "svc");
        assertThat(new ArrayList<>(seen.get(0).fields().keySet())).containsExactly("request_id", "service", "outcome");
        assertThat(seen.get(0).fields()).containsEntry("request_id", "from-event");
    }

    @Test
    void aFailingSinkDoesNotStopTheOthersOrTheCaller() {
        AuditSink broken = r -> {
            throw new IllegalStateException("boom");
        };
        List<AuditRecord> seen = capture(List.of(broken), () -> CURRENT.get().log("decrypt", "status", "success"), "svc");
        assertThat(seen).hasSize(1);
    }

    @Test
    void nullFieldValuesAreKept() throws Exception {
        Logger audit = (Logger) LoggerFactory.getLogger("audit.json");
        ListAppender<ILoggingEvent> lines = new ListAppender<>();
        lines.start();
        audit.addAppender(lines);
        try {
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("error_code", null);
            new AuditLog(List.of(new StdoutAuditSink(StdoutAuditSink.TimestampFormat.ISO_INSTANT)), "svc", "requestId")
                    .log("file_access", fields);
            JsonNode line = JSON.readTree(lines.list.get(0).getFormattedMessage());
            assertThat(line.has("error_code")).isTrue();
            assertThat(line.get("error_code").isNull()).isTrue();
        } finally {
            audit.detachAppender(lines);
        }
    }

    @Test
    void stdoutLineEnvelopeAndBothTimestampFormats() throws Exception {
        Logger audit = (Logger) LoggerFactory.getLogger("audit.json");
        ListAppender<ILoggingEvent> lines = new ListAppender<>();
        lines.start();
        audit.addAppender(lines);
        try {
            new AuditLog(List.of(new StdoutAuditSink(StdoutAuditSink.TimestampFormat.SECONDS)), "core", "x").log("encrypt", "a", 1);
            new AuditLog(List.of(new StdoutAuditSink(StdoutAuditSink.TimestampFormat.ISO_INSTANT)), "fs", "x").log("file_access", "b", 2);

            JsonNode seconds = JSON.readTree(lines.list.get(0).getFormattedMessage());
            List<String> keys = new ArrayList<>();
            seconds.fieldNames().forEachRemaining(keys::add);
            assertThat(keys).containsExactly("event", "event_type", "level", "logger", "host", "timestamp", "service", "a");
            assertThat(seconds.get("timestamp").asText()).matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z");

            JsonNode iso = JSON.readTree(lines.list.get(1).getFormattedMessage());
            assertThat(iso.get("timestamp").asText()).matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d+Z");
            assertThat(iso.get("service").asText()).isEqualTo("fs");
        } finally {
            audit.detachAppender(lines);
        }
    }

    @Test
    void autoConfigurationWiresStdoutAlwaysAndHecOnlyWhenEnabled() {
        ApplicationContextRunner runner = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ChassisAuditAutoConfiguration.class))
                .withPropertyValues("spring.application.name=demo-svc");

        runner.run(context -> {
            assertThat(context).hasSingleBean(AuditLog.class).hasSingleBean(StdoutAuditSink.class)
                    .doesNotHaveBean(SplunkHecSink.class);
            assertThat(context.getBean(AuditLog.class).service()).isEqualTo("demo-svc");
        });
        runner.withPropertyValues("hsm.chassis.audit.hec.enabled=true", "hsm.chassis.audit.hec.url=http://127.0.0.1:9/hec",
                        "hsm.chassis.audit.hec.token=t", "hsm.chassis.audit.hec.flush-interval-seconds=3600")
                .run(context -> assertThat(context).hasSingleBean(SplunkHecSink.class));
        runner.withPropertyValues("hsm.chassis.audit.service=override")
                .run(context -> assertThat(context.getBean(AuditLog.class).service()).isEqualTo("override"));
    }
}
