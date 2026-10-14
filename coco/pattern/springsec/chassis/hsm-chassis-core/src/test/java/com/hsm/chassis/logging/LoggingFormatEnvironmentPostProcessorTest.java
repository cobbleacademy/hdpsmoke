package com.hsm.chassis.logging;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.logging.DeferredLogFactory;
import org.springframework.core.env.MapPropertySource;
import org.springframework.mock.env.MockEnvironment;

import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class LoggingFormatEnvironmentPostProcessorTest {

    private static final DeferredLogFactory LOGS = Supplier::get;

    private static MockEnvironment process(MockEnvironment env) {
        new LoggingFormatEnvironmentPostProcessor(LOGS).postProcessEnvironment(env, new SpringApplication());
        return env;
    }

    @Test
    void textByDefaultAndNothingElseIsSet() {
        MockEnvironment env = process(new MockEnvironment());
        assertThat(env.getProperty(LoggingFormatEnvironmentPostProcessor.RESOLVED_FORMAT_PROPERTY)).isEqualTo("text");
        assertThat(env.getProperty("logging.structured.format.console")).isNull();
        assertThat(env.getProperty("spring.main.banner-mode")).isNull();
    }

    @Test
    void jsonSetsTheStructuredDefaults() {
        MockEnvironment env = process(new MockEnvironment()
                .withProperty("hsm.chassis.logging.format", "json")
                .withProperty("spring.application.name", "demo-svc")
                .withProperty("hsm.chassis.web.request-id.mdc-key", "correlationId"));

        assertThat(env.getProperty(LoggingFormatEnvironmentPostProcessor.RESOLVED_FORMAT_PROPERTY)).isEqualTo("json");
        assertThat(env.getProperty("logging.structured.format.console")).isEqualTo("logstash");
        assertThat(env.getProperty("logging.structured.json.rename[correlationId]")).isEqualTo("request_id");
        assertThat(env.getProperty("logging.structured.json.rename[@timestamp]")).isEqualTo("timestamp");
        assertThat(env.getProperty("logging.structured.json.add.service")).isEqualTo("demo-svc");
        assertThat(env.getProperty("logging.structured.json.stacktrace.max-length")).isEqualTo("4000");
        assertThat(env.getProperty("spring.main.banner-mode")).isEqualTo("off");
    }

    @Test
    void envAliasAndCaseAreAccepted() {
        MockEnvironment env = process(new MockEnvironment().withProperty("HSM_LOG_FORMAT", " JSON "));
        assertThat(env.getProperty(LoggingFormatEnvironmentPostProcessor.RESOLVED_FORMAT_PROPERTY)).isEqualTo("json");
        assertThat(env.getProperty("logging.structured.json.rename[requestId]")).isEqualTo("request_id");
    }

    @Test
    void anUnknownValueFallsBackToText() {
        MockEnvironment env = process(new MockEnvironment().withProperty("hsm.chassis.logging.format", "jsn"));
        assertThat(env.getProperty(LoggingFormatEnvironmentPostProcessor.RESOLVED_FORMAT_PROPERTY)).isEqualTo("text");
        assertThat(env.getProperty("logging.structured.format.console")).isNull();
    }

    @Test
    void serviceComesFromTheAuditSettingAndExplicitSettingsWin() {
        MockEnvironment env = new MockEnvironment()
                .withProperty("hsm.chassis.logging.format", "json")
                .withProperty("spring.application.name", "hsm-core-service")
                .withProperty("hsm.chassis.audit.service", "hsm-bulk-service");
        env.getPropertySources().addFirst(new MapPropertySource("service-own",
                Map.of("logging.structured.json.stacktrace.max-length", "1000")));
        process(env);

        assertThat(env.getProperty("logging.structured.json.add.service")).isEqualTo("hsm-bulk-service");
        assertThat(env.getProperty("logging.structured.json.stacktrace.max-length")).isEqualTo("1000");
    }
}
