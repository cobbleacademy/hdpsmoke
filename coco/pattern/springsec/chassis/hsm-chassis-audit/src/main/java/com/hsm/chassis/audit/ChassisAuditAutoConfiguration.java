package com.hsm.chassis.audit;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import java.util.List;

/**
 * Audit for every service on the chassis: the {@link AuditLog} API, the stdout
 * {@code audit.json} sink, and -- only when {@code hsm.chassis.audit.hec.enabled=true} --
 * Splunk HEC delivery. Each backs off if the service defines its own bean (hsm-core-service
 * defines its own {@link SplunkHecSink} to resolve the HEC token from Key Vault).
 *
 * <pre>
 * hsm.chassis.audit.service            default spring.application.name
 * hsm.chassis.audit.timestamp-format   iso-instant (default) | seconds
 * hsm.chassis.audit.hec.enabled        false
 * hsm.chassis.audit.hec.url / token / index (hsm_audit) / source (= service) / sourcetype (_json)
 * hsm.chassis.audit.hec.batch-size (50) / flush-interval-seconds (5)
 * hsm.chassis.audit.hec.max-queue-size (10000) / shutdown-flush-seconds (10)
 * </pre>
 * The request id comes from the MDC key the chassis request-id filter uses
 * ({@code hsm.chassis.web.request-id.mdc-key}, default {@code requestId}).
 */
@AutoConfiguration
public class ChassisAuditAutoConfiguration {

    static final String PREFIX = "hsm.chassis.audit.";

    @Bean
    @ConditionalOnMissingBean
    public StdoutAuditSink stdoutAuditSink(Environment env) {
        String format = env.getProperty(PREFIX + "timestamp-format", "iso-instant").trim();
        return new StdoutAuditSink("seconds".equalsIgnoreCase(format)
                ? StdoutAuditSink.TimestampFormat.SECONDS
                : StdoutAuditSink.TimestampFormat.ISO_INSTANT);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = PREFIX + "hec.enabled", havingValue = "true")
    public SplunkHecSink splunkHecSink(Environment env) {
        String service = service(env);
        return new SplunkHecSink(new SplunkHecSink.Settings(
                env.getRequiredProperty(PREFIX + "hec.url"),
                env.getProperty(PREFIX + "hec.index", "hsm_audit"),
                env.getProperty(PREFIX + "hec.source", service),
                env.getProperty(PREFIX + "hec.sourcetype", "_json"),
                env.getProperty(PREFIX + "hec.batch-size", Integer.class, 50),
                env.getProperty(PREFIX + "hec.flush-interval-seconds", Integer.class, 5),
                env.getProperty(PREFIX + "hec.max-queue-size", Integer.class, 10_000),
                env.getProperty(PREFIX + "hec.shutdown-flush-seconds", Integer.class, 10)),
                env.getRequiredProperty(PREFIX + "hec.token"));
    }

    @Bean
    @ConditionalOnMissingBean
    public AuditLog auditLog(ObjectProvider<AuditSink> sinks, Environment env) {
        List<AuditSink> ordered = sinks.orderedStream().toList();
        return new AuditLog(ordered, service(env), env.getProperty("hsm.chassis.web.request-id.mdc-key", "requestId"));
    }

    private static String service(Environment env) {
        return env.getProperty(PREFIX + "service", env.getProperty("spring.application.name", "unknown"));
    }
}
