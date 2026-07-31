package com.hsm.bulk.config;

import com.hsm.bulk.auth.JwtValidator;
import com.hsm.bulk.auth.MockJwtValidator;
import com.hsm.bulk.auth.RsaJwtValidator;
import com.hsm.bulk.crypto.AzureKeyVaultKekClient;
import com.hsm.bulk.crypto.KekClient;
import com.hsm.bulk.crypto.MockKekClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Mirrors com.hsm.core.config.AuthBeansConfig/CryptoBeansConfig's mock-vs-real
 * selection pattern, driven by the single hsm.mock-kek flag (HsmBulkProperties):
 * true swaps BOTH the KekClient (MockKekClient, no real Key Vault call) and the
 * JwtValidator (MockJwtValidator, fixed demo-token table matching
 * hsm-core-service's own demo tokens) -- letting BulkVsBatchBenchmark run
 * entirely locally. false wires the real Azure/JWKS-backed implementations,
 * identical in shape to hsm-core-service's own production wiring.
 */
@Configuration
public class CryptoBeansConfig {

    static {
        FipsBootstrap.register();
    }

    @Bean(destroyMethod = "close")
    public KekClient kekClient(HsmBulkProperties properties) {
        if (properties.mockKek()) {
            return new MockKekClient();
        }
        return new AzureKeyVaultKekClient(properties);
    }

    @Bean
    public JwtValidator jwtValidator(HsmBulkProperties properties) {
        if (properties.mockKek()) {
            return new MockJwtValidator();
        }
        return new RsaJwtValidator(properties.jwt());
    }
}
