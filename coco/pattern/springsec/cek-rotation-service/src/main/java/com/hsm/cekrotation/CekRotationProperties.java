package com.hsm.cekrotation;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Ported from cek_rotation/config.py's Settings. */
@ConfigurationProperties(prefix = "cek")
public record CekRotationProperties(
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
