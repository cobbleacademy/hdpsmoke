package com.hsm.encryption.config;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

/**
 * Ports app/config.py's model_validator checks: certain fields are required
 * unless demo_mode is true, and SPLUNK_HEC_URL is required when Splunk is enabled.
 */
@Component
public class HsmPropertiesValidator {

    private final HsmProperties properties;

    public HsmPropertiesValidator(HsmProperties properties) {
        this.properties = properties;
    }

    @PostConstruct
    public void validate() {
        if (!properties.demoMode()) {
            require(!blank(properties.azure().keyvaultUrl()), "AZURE_KEYVAULT_URL is required unless DEMO_MODE=true");
            require(!blank(properties.database().url()), "DATABASE_URL is required unless DEMO_MODE=true");
            require(!blank(properties.jwt().issuer()), "JWT_ISSUER is required unless DEMO_MODE=true");
            require(!blank(properties.jwt().publicKeyPem()) || !blank(properties.jwt().jwksUrl()),
                    "Either JWT_PUBLIC_KEY_PEM or JWT_JWKS_URL must be set unless DEMO_MODE=true");
        }
        if (properties.splunk().enabled()) {
            require(!blank(properties.splunk().hecUrl()), "SPLUNK_HEC_URL is required when SPLUNK_ENABLED=true");
        }
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
