package com.hsm.core.config;

import com.hsm.core.crypto.AzureKeyVaultKekClient;
import com.hsm.core.crypto.DekCache;
import com.hsm.core.crypto.KekClient;
import com.hsm.core.crypto.MockKekClient;
import com.hsm.core.crypto.NullDekCache;
import com.hsm.core.crypto.RedisDekCache;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Base64;
import java.util.HashSet;
import java.util.Set;

/** Ported from app/dependencies.py's init_dependencies -- KEK client / DEK cache singletons. */
@Configuration
public class CryptoBeansConfig {

    private static final Logger log = LoggerFactory.getLogger(CryptoBeansConfig.class);

    // BC-FIPS is registered (JVM-wide approved-only) by hsm-chassis-core's
    // FipsEnvironmentPostProcessor before any bean definition is read -- including under
    // @SpringBootTest -- so the eager BC-FIPS users below (MockKekClient,
    // AzureKeyVaultKekClient, RedisDekCache) never construct ahead of it.

    @Bean(destroyMethod = "close")
    public KekClient kekClient(HsmProperties properties) {
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
    public DekCache dekCache(HsmProperties properties, KekClient kekClient) {
        if (!properties.dekCache().enabled() || properties.redis().url().isBlank()) {
            return new NullDekCache();
        }
        return buildRedisDekCache(properties, kekClient);
    }

    private DekCache buildRedisDekCache(HsmProperties properties, KekClient kekClient) {
        HsmProperties.DekCache cfg = properties.dekCache();

        // 1. Fetch current_key pointer ("alpha"/"beta") from KV Secrets.
        String currentSlot = kekClient.fetchSecret(cfg.cekCurrentKeySecretName()).strip();

        // 2. Fetch active slot bytes + kv_version. kv_version namespaces Redis keys as
        //    {slot}:{kv_version}:{edek_id}, preventing cross-pod collisions when alpha
        //    is reused after alpha->beta->alpha.
        String activeSecretName = slotSecretName(cfg, currentSlot);
        KekClient.SecretWithVersion active = kekClient.fetchSecretWithVersion(activeSecretName);
        byte[] cek = Base64.getDecoder().decode(active.value());

        // 3. Load the inactive slot as the previous CEK fallback so entries written
        //    before rotation stay readable during the convergence window.
        byte[] prevCek = null;
        String prevComposite = null;
        try {
            String inactiveSlot = "alpha".equals(currentSlot) ? "beta" : "alpha";
            KekClient.SecretWithVersion prev = kekClient.fetchSecretWithVersion(slotSecretName(cfg, inactiveSlot));
            prevCek = Base64.getDecoder().decode(prev.value());
            prevComposite = inactiveSlot + ":" + prev.kvVersion();
        } catch (Exception e) {
            // inactive slot may not exist yet on very first deployment
        }

        RedisClient client = RedisClient.create(properties.redis().url());
        StatefulRedisConnection<byte[], byte[]> connection = client.connect(ByteArrayCodec.INSTANCE);

        Set<String> excluded = new HashSet<>();
        for (String c : cfg.excludedClassifications().split(",")) {
            if (!c.isBlank()) {
                excluded.add(c.strip().toLowerCase());
            }
        }

        return new RedisDekCache(
                connection.sync(),
                cek,
                currentSlot + ":" + active.kvVersion(),
                cfg.ttlSeconds(),
                excluded,
                prevCek,
                prevComposite
        );
    }

    private static String slotSecretName(HsmProperties.DekCache cfg, String slot) {
        return "alpha".equals(slot) ? cfg.cekAlphaSecretName() : cfg.cekBetaSecretName();
    }
}
