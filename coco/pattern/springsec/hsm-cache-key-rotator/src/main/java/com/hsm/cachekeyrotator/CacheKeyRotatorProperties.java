package com.hsm.cachekeyrotator;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Ported from cek_rotation/config.py's Settings. */
@ConfigurationProperties(prefix = "cache-key-rotator")
public record CacheKeyRotatorProperties(
        String azureKeyvaultSecretUrl,
        String cekAlphaSecretName,
        String cekBetaSecretName,
        String currentKeySecretName,
        int rotationIntervalHours,
        String redisUrl,
        String redisPostRotationMode, // "none" | "flush" | "rekey"
        int dekCacheTtlSeconds
) {
}
