package com.hsm.cekrotation;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Wires the Azure Secrets client and (optionally) Redis, then runs one rotation
 * immediately at startup and repeats every rotation-interval-hours (fixed
 * <b>delay</b>, not fixed rate -- Python sleeps after each run completes, same
 * semantics). Ported from cek_rotation/main.py's run().
 */
@Component
public class RotationRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(RotationRunner.class);

    private final CekRotationProperties config;
    private final TaskScheduler taskScheduler;

    private RedisClient redisClient;
    private StatefulRedisConnection<byte[], byte[]> redisConnection;
    private CekRotator rotator;

    public RotationRunner(CekRotationProperties config, TaskScheduler taskScheduler) {
        this.config = config;
        this.taskScheduler = taskScheduler;
    }

    @Override
    public void run(ApplicationArguments args) {
        SecretClient secretClient = new SecretClientBuilder()
                .vaultUrl(config.azureKeyvaultSecretUrl())
                .credential(new DefaultAzureCredentialBuilder().build())
                .buildClient();

        RedisOps redisOps = null;
        if (!config.redisUrl().isBlank() && !"none".equals(config.redisPostRotationMode())) {
            redisClient = RedisClient.create(config.redisUrl());
            redisConnection = redisClient.connect(ByteArrayCodec.INSTANCE);
            redisOps = new RedisOps(redisConnection.sync());
        }

        rotator = new CekRotator(secretClient, config, redisOps);

        log.info("cek_rotation_service_started vault_url={} interval_hours={} redis_mode={}",
                config.azureKeyvaultSecretUrl(), config.rotationIntervalHours(), config.redisPostRotationMode());

        Duration interval = Duration.ofHours(config.rotationIntervalHours());
        taskScheduler.scheduleWithFixedDelay(this::runOnce, Instant.now(), interval);
    }

    private void runOnce() {
        try {
            rotator.rotate();
        } catch (Exception e) {
            log.error("rotation_cycle_failed error={}", e.getMessage(), e);
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("cek_rotation_service_stopping");
        if (redisConnection != null) {
            redisConnection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
        log.info("cek_rotation_service_stopped");
    }
}
