package com.hsm.encryption.config;

import com.hsm.encryption.crypto.AzureKeyVaultKekClient;
import com.hsm.encryption.crypto.DekCache;
import com.hsm.encryption.crypto.KekClient;
import com.hsm.encryption.crypto.MockKekClient;
import com.hsm.encryption.crypto.NullDekCache;
import com.hsm.encryption.crypto.RedisDekCache;
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

    // HsmEncryptionServiceApplication's own static initializer only reliably runs
    // before SpringApplication.run() on a real `java -jar` launch -- referencing
    // that class as a @SpringBootTest config *source* (a Class object) does not
    // by itself trigger its <clinit>. In a Spring test context, whichever
    // @Configuration class's @Bean method Spring happens to invoke first is free
    // to run before that class is ever actively used. This class's own bean
    // methods construct BC-FIPS ciphers/SecureRandoms eagerly (MockKekClient,
    // AzureKeyVaultKekClient, RedisDekCache), so registering here too guarantees
    // readiness via the JVM's own per-class init-before-use contract, independent
    // of which @Configuration class Spring resolves first.
    static {
        FipsBootstrap.register();
    }

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
