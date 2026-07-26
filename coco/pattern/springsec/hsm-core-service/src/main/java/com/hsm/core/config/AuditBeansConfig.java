package com.hsm.core.config;

import com.hsm.core.audit.SplunkHecBatcher;
import com.hsm.core.crypto.KekClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Ported from app/main.py's lifespan: fetches the Splunk HEC token from Key Vault
 * (secret name "splunk-hec-token") so it never lives in plain env vars in production,
 * when splunk.enabled but no static SPLUNK_HEC_TOKEN was configured and not demo mode.
 */
@Configuration
public class AuditBeansConfig {

    @Bean
    public SplunkHecBatcher splunkHecBatcher(HsmProperties properties, KekClient kekClient) {
        String token = properties.splunk().hecToken();
        if (properties.splunk().enabled() && token.isBlank() && !properties.demoMode()) {
            token = kekClient.fetchSecret("splunk-hec-token");
        }
        SplunkHecBatcher batcher = new SplunkHecBatcher(properties.splunk(), token);
        batcher.start();
        return batcher;
    }
}
