package com.hsm.bulk.config;

import com.hsm.bulk.auth.JwtValidator;
import com.hsm.bulk.auth.MockJwtValidator;
import com.hsm.bulk.auth.RsaJwtValidator;
import com.hsm.bulk.crypto.AzureKeyVaultKekClient;
import com.hsm.bulk.crypto.KekClient;
import com.hsm.bulk.crypto.MockKekClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Mirrors com.hsm.core.config.AuthBeansConfig/CryptoBeansConfig's mock-vs-real
 * selection pattern, and now the exact same flag names: hsm.demo-mode
 * (HsmBulkProperties) swaps BOTH the KekClient (MockKekClient, no real Key
 * Vault call) and the JwtValidator (MockJwtValidator, fixed demo-token table
 * matching hsm-core-service's own demo tokens) -- letting BulkVsBatchBenchmark
 * run entirely locally. hsm.skip-akv is a second, independent lever -- same
 * relationship as HsmProperties' demoMode/skipAkv -- that only ever affects
 * the KekClient, so demoMode=false + skipAkv=true gets real JWT/scope
 * validation without needing a reachable Key Vault/Managed HSM.
 */
@Configuration
public class CryptoBeansConfig {

    private static final Logger log = LoggerFactory.getLogger(CryptoBeansConfig.class);

    static {
        FipsBootstrap.register();
    }

    @Bean(destroyMethod = "close")
    public KekClient kekClient(HsmBulkProperties properties) {
        if (properties.demoMode()) {
            return new MockKekClient();
        }
        if (properties.skipAkv()) {
            log.warn("SKIP_AKV=true -- using mock KEK client; encrypt/decrypt use in-memory keys, NOT production-safe");
            return new MockKekClient();
        }
        return new AzureKeyVaultKekClient(properties);
    }

    @Bean
    public JwtValidator jwtValidator(HsmBulkProperties properties) {
        if (properties.demoMode()) {
            return new MockJwtValidator();
        }
        return new RsaJwtValidator(properties.jwt());
    }
}
