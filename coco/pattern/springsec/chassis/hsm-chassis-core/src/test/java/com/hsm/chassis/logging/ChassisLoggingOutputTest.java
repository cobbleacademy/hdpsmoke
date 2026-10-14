package com.hsm.chassis.logging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The real thing: a Spring Boot application whose logback-spring.xml is only the chassis
 * include (src/test/resources/logback-spring.xml, as in every service), run in both formats.
 * The audit line must come out byte for byte the same in both.
 */
@ExtendWith(OutputCaptureExtension.class)
class ChassisLoggingOutputTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String AUDIT_LINE =
            "{\"event\":\"encrypt\",\"event_type\":\"encrypt\",\"level\":\"info\",\"request_id\":\"req-1\"}";

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class PlainApp {
    }

    private static void runAndLog(String... properties) {
        try (ConfigurableApplicationContext ignored = new SpringApplicationBuilder(PlainApp.class)
                .web(WebApplicationType.NONE)
                .properties("spring.application.name=demo-svc", "hsm.chassis.web.request-id.mdc-key=correlationId")
                .properties(properties)
                .run()) {
            Logger app = LoggerFactory.getLogger("com.hsm.demo.Probe");
            MDC.put("correlationId", "req-1");
            try {
                app.info("probe_event key=value");
                app.error("probe_failure", new IllegalStateException("boom"));
                LoggerFactory.getLogger("audit.json").info(AUDIT_LINE);
            } finally {
                MDC.clear();
            }
        }
    }

    @Test
    void textIsSpringBootsDefaultLine(CapturedOutput output) {
        runAndLog();

        String probe = output.getOut().lines().filter(l -> l.contains("probe_event")).findFirst().orElseThrow();
        assertThat(probe).matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}\\S*\\s+INFO \\d+ --- \\[demo-svc] \\[\\s*main] "
                + "com\\.hsm\\.demo\\.Probe\\s+: probe_event key=value");
        assertThat(output.getOut()).contains("java.lang.IllegalStateException: boom");
        assertThat(output.getOut().lines()).contains(AUDIT_LINE);
    }

    @Test
    void jsonIsOneObjectPerLineWithTheAgreedFields(CapturedOutput output) throws Exception {
        runAndLog("hsm.chassis.logging.format=json");

        List<JsonNode> events = new ArrayList<>();
        for (String line : output.getOut().lines().filter(l -> !l.isBlank()).toList()) {
            events.add(JSON.readTree(line)); // throws if any stdout line is not JSON (banner, stack-trace lines)
        }

        JsonNode probe = events.stream().filter(e -> "probe_event key=value".equals(e.path("message").asText()))
                .findFirst().orElseThrow();
        List<String> fields = new ArrayList<>();
        probe.fieldNames().forEachRemaining(fields::add);
        assertThat(fields).containsExactlyInAnyOrder(
                "timestamp", "message", "logger", "thread", "level", "request_id", "service", "host");
        assertThat(probe.get("request_id").asText()).isEqualTo("req-1");
        assertThat(probe.get("service").asText()).isEqualTo("demo-svc");
        assertThat(probe.get("logger").asText()).isEqualTo("com.hsm.demo.Probe");
        assertThat(probe.get("level").asText()).isEqualTo("INFO");

        JsonNode failure = events.stream().filter(e -> "probe_failure".equals(e.path("message").asText()))
                .findFirst().orElseThrow();
        assertThat(failure.get("stack_trace").asText()).startsWith("java.lang.IllegalStateException: boom")
                .hasSizeLessThanOrEqualTo(LoggingFormatEnvironmentPostProcessor.STACK_TRACE_MAX_LENGTH);

        assertThat(output.getOut().lines()).contains(AUDIT_LINE);
    }
}
