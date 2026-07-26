package com.hsm.core.crypto;

import com.hsm.core.config.HsmProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Base64;

/**
 * Polls Azure KV Secrets every dek-cache.reload-interval-seconds. Detects a slot
 * change OR kv_version change (same slot, new bytes written by the CEK rotation
 * service) and calls DekCache.rotate() in-process -- no pod restart needed. All
 * pods converge within one poll interval, well within the cache TTL. Ported from
 * app/dependencies.py's _cek_reload_loop; no-op unless the DekCache bean is
 * actually a RedisDekCache (dek-cache.enabled=true and redis.url set).
 */
@Component
public class CekHotReloadScheduler {

    private static final Logger log = LoggerFactory.getLogger(CekHotReloadScheduler.class);

    private final DekCache dekCache;
    private final KekClient kekClient;
    private final HsmProperties.DekCache config;
    private final TaskScheduler taskScheduler;

    public CekHotReloadScheduler(DekCache dekCache, KekClient kekClient, HsmProperties properties, TaskScheduler taskScheduler) {
        this.dekCache = dekCache;
        this.kekClient = kekClient;
        this.config = properties.dekCache();
        this.taskScheduler = taskScheduler;
    }

    @PostConstruct
    public void start() {
        if (!(dekCache instanceof RedisDekCache)) {
            return;
        }
        Duration interval = Duration.ofSeconds(config.reloadIntervalSeconds());
        taskScheduler.scheduleWithFixedDelay(this::reload, Instant.now().plus(interval), interval);
    }

    private void reload() {
        RedisDekCache redisDekCache = (RedisDekCache) dekCache;
        try {
            String latestSlot = kekClient.fetchSecret(config.cekCurrentKeySecretName()).strip();
            String latestSecretName = "alpha".equals(latestSlot) ? config.cekAlphaSecretName() : config.cekBetaSecretName();
            KekClient.SecretWithVersion latest = kekClient.fetchSecretWithVersion(latestSecretName);
            String latestComposite = latestSlot + ":" + latest.kvVersion();
            if (!latestComposite.equals(redisDekCache.getCurrentVersion())) {
                byte[] newCek = Base64.getDecoder().decode(latest.value());
                redisDekCache.rotate(newCek, latestComposite);
                log.info("cek_rotated new_version={}", latestComposite);
            }
        } catch (Exception e) {
            log.warn("CEK reload poll failed: {}", e.getMessage());
        }
    }
}
