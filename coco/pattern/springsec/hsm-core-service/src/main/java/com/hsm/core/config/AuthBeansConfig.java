package com.hsm.core.config;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hsm.core.auth.AppRegistryService;
import com.hsm.core.auth.JwtValidator;
import com.hsm.core.auth.MockJwtValidator;
import com.hsm.core.auth.NullPbacClient;
import com.hsm.core.auth.PbacClient;
import com.hsm.core.auth.PbacIntegrationConfigLoader;
import com.hsm.core.auth.PlainIdPbacClient;
import com.hsm.core.auth.RsaJwtValidator;
import com.hsm.core.auth.SelfIssuedRoutingJwtValidator;
import com.hsm.core.auth.SelfSignedAppKeyJwtValidator;
import com.hsm.core.crypto.KekClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Ported from app/dependencies.py's init_dependencies -- JWT validator / PBAC client singletons. */
@Configuration
public class AuthBeansConfig {

    private static final Logger log = LoggerFactory.getLogger(AuthBeansConfig.class);

    /**
     * Wraps whichever "primary" validator the mode calls for (MockJwtValidator in
     * demo, RsaJwtValidator in production) in SelfIssuedRoutingJwtValidator, so a
     * self-issued, registered-key-verified bearer token (SelfSignedAppKeyJwtValidator)
     * is accepted alongside the primary mechanism in both modes -- not a separate
     * opt-in, since routing is decided per-request by the token's own shape/claims,
     * not by a deployment-wide switch.
     */
    @Bean
    public JwtValidator jwtValidator(HsmProperties properties, AppRegistryService appRegistry) {
        JwtValidator primary = properties.demoMode() ? new MockJwtValidator() : new RsaJwtValidator(properties.jwt());
        SelfSignedAppKeyJwtValidator selfSigned = new SelfSignedAppKeyJwtValidator(appRegistry, properties.jwt());
        return new SelfIssuedRoutingJwtValidator(primary, selfSigned, properties);
    }

    @Bean
    public PbacClient pbacClient(HsmProperties properties, KekClient kekClient) {
        HsmProperties.Pbac pbac = properties.pbac();
        if (properties.demoMode() || !pbac.enabled() || pbac.plainidUrl().isBlank()) {
            return new NullPbacClient();
        }
        String apiKey = kekClient.fetchSecret(pbac.plainidApiKeySecretName());
        ObjectNode integrationConfig = PbacIntegrationConfigLoader.load(pbac.integrationConfigPath());
        log.info("pbac_enabled plainid_url={}", pbac.plainidUrl());
        return new PlainIdPbacClient(
                pbac.plainidUrl(),
                apiKey,
                integrationConfig,
                pbac.cacheTtlSeconds(),
                pbac.failOpen(),
                pbac.httpTimeoutSeconds()
        );
    }
}
