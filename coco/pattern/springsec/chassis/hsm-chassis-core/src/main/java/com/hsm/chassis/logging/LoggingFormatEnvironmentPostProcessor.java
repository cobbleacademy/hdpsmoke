package com.hsm.chassis.logging;

import org.apache.commons.logging.Log;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.logging.DeferredLogFactory;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Chooses the application-log format for the shared logback configuration
 * ({@code hsm/chassis/logback/base.xml}) before the logging system starts (OBS-1).
 *
 * <pre>
 * hsm.chassis.logging.format   text (default) | json      env alias: HSM_LOG_FORMAT
 * </pre>
 *
 * <ul>
 *   <li>{@code text}: Spring Boot's default console line, exactly as before the chassis.</li>
 *   <li>{@code json}: one JSON object per line via Boot's built-in structured logging
 *       (logstash format), with field names aligned to the audit lines: {@code timestamp},
 *       {@code message}, {@code logger}, {@code thread}, {@code level}, {@code request_id}
 *       (from the chassis request-id MDC key), {@code service}, {@code host} (the pod
 *       name), and {@code stack_trace} capped at 4000 characters. The start-up banner is
 *       off so stdout carries nothing but JSON.</li>
 * </ul>
 *
 * Any other value logs a warning and falls back to {@code text}. Every property set here
 * is a default: a service's own {@code logging.structured.*} or
 * {@code spring.main.banner-mode} setting wins. The audit stream ({@code audit.json}) is
 * not affected by either format.
 */
public class LoggingFormatEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    public static final String FORMAT_PROPERTY = "hsm.chassis.logging.format";
    public static final String FORMAT_ENV_ALIAS = "HSM_LOG_FORMAT";
    /** Read by base.xml: always {@code text} or {@code json}. */
    public static final String RESOLVED_FORMAT_PROPERTY = "hsm.chassis.logging.resolved-format";
    static final String PROPERTY_SOURCE_NAME = "hsmChassisLoggingDefaults";
    static final int STACK_TRACE_MAX_LENGTH = 4000;

    private final Log log;

    public LoggingFormatEnvironmentPostProcessor(DeferredLogFactory logFactory) {
        this.log = logFactory.getLog(LoggingFormatEnvironmentPostProcessor.class);
    }

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        String requested = environment.getProperty(FORMAT_PROPERTY, environment.getProperty(FORMAT_ENV_ALIAS, "text"));
        String format = requested.trim().toLowerCase(Locale.ROOT);
        if (!format.equals("text") && !format.equals("json")) {
            log.warn("logging_format_invalid value=" + requested + " -- using text (allowed: text, json)");
            format = "text";
        }

        Map<String, Object> defaults = new LinkedHashMap<>();
        defaults.put(RESOLVED_FORMAT_PROPERTY, format);
        if (format.equals("json")) {
            String requestIdKey = environment.getProperty("hsm.chassis.web.request-id.mdc-key", "requestId");
            String service = environment.getProperty("hsm.chassis.audit.service",
                    environment.getProperty("spring.application.name", "unknown"));
            defaults.put("logging.structured.format.console", "logstash");
            defaults.put("logging.structured.json.rename[@timestamp]", "timestamp");
            defaults.put("logging.structured.json.rename[logger_name]", "logger");
            defaults.put("logging.structured.json.rename[thread_name]", "thread");
            defaults.put("logging.structured.json.rename[" + requestIdKey + "]", "request_id");
            defaults.put("logging.structured.json.exclude", "@version,level_value,tags");
            defaults.put("logging.structured.json.add.service", service);
            defaults.put("logging.structured.json.add.host", "${HOSTNAME:unknown}");
            defaults.put("logging.structured.json.stacktrace.max-length", String.valueOf(STACK_TRACE_MAX_LENGTH));
            defaults.put("spring.main.banner-mode", "off");
        }
        // Lowest precedence: anything the service or its deployment sets explicitly wins.
        environment.getPropertySources().addLast(new MapPropertySource(PROPERTY_SOURCE_NAME, defaults));
    }

    @Override
    public int getOrder() {
        // After the config files are loaded (so the format and the request-id key are
        // visible), before the logging system initializes.
        return Ordered.LOWEST_PRECEDENCE;
    }
}
