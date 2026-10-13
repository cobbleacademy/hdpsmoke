package com.hsm.core.config;

import com.hsm.chassis.audit.SplunkHecSink;
import com.hsm.core.crypto.KekClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Splunk HEC delivery for audit events, when {@code hsm.splunk.enabled=true}: the chassis
 * {@link SplunkHecSink}, configured from this service's existing {@code hsm.splunk.*}
 * settings (so the charts' SPLUNK_* variables are unchanged). Defined here rather than
 * left to hsm-chassis-audit's auto-configuration because the token may live in Key Vault:
 * ported from app/main.py's lifespan, it is fetched (secret "splunk-hec-token") when no
 * static SPLUNK_HEC_TOKEN is configured and not in demo mode, so it never sits in a plain
 * env var in production.
 */
@Configuration
public class AuditBeansConfig {

    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnProperty(name = "hsm.splunk.enabled", havingValue = "true")
    public SplunkHecSink splunkHecSink(HsmProperties properties, KekClient kekClient) {
        HsmProperties.Splunk splunk = properties.splunk();
        String token = splunk.hecToken();
        if (token.isBlank() && !properties.demoMode()) {
            token = kekClient.fetchSecret("splunk-hec-token");
        }
        return new SplunkHecSink(new SplunkHecSink.Settings(
                splunk.hecUrl(), splunk.index(), splunk.source(), splunk.sourcetype(),
                splunk.batchSize(), splunk.flushIntervalSeconds(), splunk.maxQueueSize(), splunk.shutdownFlushSeconds()),
                token);
    }
}
