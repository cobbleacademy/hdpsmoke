package com.hsm.cekrotation;

import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The CEK rotation algorithm, ported from cek_rotation/rotator.py's rotate_cek().
 * Rotates the alpha/beta Key Vault secret slots and (best-effort) migrates the
 * Redis DEK cache so main-service pods converge onto the new CEK within one poll
 * interval, without a restart.
 */
public class CekRotator {

    private static final Logger log = LoggerFactory.getLogger(CekRotator.class);
    // FIPS-approved-mode DRBG-backed SecureRandom -- this mints the actual CEK bytes,
    // so it gets the same treatment as every other key/IV generation site in both modules.
    private static final SecureRandom RANDOM = CryptoServicesRegistrar.getSecureRandom();

    private final SecretClient secretClient;
    private final CekRotationProperties config;
    private final RedisOps redisOps; // null when Redis is not configured

    public CekRotator(SecretClient secretClient, CekRotationProperties config, RedisOps redisOps) {
        this.secretClient = secretClient;
        this.config = config;
        this.redisOps = redisOps;
    }

    public Map<String, Object> rotate() {
        long start = System.currentTimeMillis();

        // 1. Discover active slot.
        String activeSlot = secretClient.getSecret(config.currentKeySecretName()).getValue().trim().toLowerCase();
        if (!activeSlot.equals("alpha") && !activeSlot.equals("beta")) {
            throw new IllegalStateException("current_key secret must be 'alpha' or 'beta', got: " + activeSlot);
        }

        // 2. Determine target (inactive) slot.
        String inactiveSlot = activeSlot.equals("alpha") ? "beta" : "alpha";
        String inactiveSecretName = slotSecretName(inactiveSlot);

        // 3. (rekey mode only) Read old CEK bytes + kv_version before overwrite -- best-effort.
        byte[] oldCekBytes = null;
        String oldKvVersion = null;
        boolean doRekey = redisOps != null && "rekey".equals(config.redisPostRotationMode());
        if (doRekey) {
            try {
                KeyVaultSecret activeSecret = secretClient.getSecret(slotSecretName(activeSlot));
                oldCekBytes = Base64.getDecoder().decode(activeSecret.getValue());
                oldKvVersion = lastPathSegment(activeSecret.getProperties().getId());
            } catch (Exception e) {
                log.warn("failed to read active slot before rotation, falling back to non-rekey: {}", e.getMessage());
                doRekey = false;
            }
        }

        // 4. Generate new CEK (256-bit random key).
        byte[] newCekBytes = new byte[32];
        RANDOM.nextBytes(newCekBytes);
        String newCekB64 = Base64.getEncoder().encodeToString(newCekBytes);

        // 5. Write new CEK to inactive slot.
        KeyVaultSecret written = secretClient.setSecret(inactiveSecretName, newCekB64);

        // 6. Extract kv_version.
        String kvVersion = lastPathSegment(written.getProperties().getId());

        // 7. Flip active-slot pointer. Main service pods pick this up on their next poll.
        secretClient.setSecret(config.currentKeySecretName(), inactiveSlot);

        // 8. Post-rotation Redis ops -- wrapped so Redis failures never fail the overall rotation.
        Map<String, Object> redisOpsResult = new LinkedHashMap<>();
        if (redisOps != null && !"none".equals(config.redisPostRotationMode())) {
            try {
                String oldVersion = oldKvVersion != null ? activeSlot + ":" + oldKvVersion : null;
                String newVersion = inactiveSlot + ":" + kvVersion;

                if (doRekey) {
                    RedisOps.RekeyResult rekeyResult =
                            redisOps.rekeyDekCache(oldCekBytes, newCekBytes, oldVersion, newVersion, config.dekCacheTtlSeconds());
                    redisOpsResult.put("rekey", Map.of(
                            "rekeyed", rekeyResult.rekeyed(), "skipped", rekeyResult.skipped(), "failed", rekeyResult.failed()));
                } else if ("flush".equals(config.redisPostRotationMode())) {
                    redisOpsResult.put("flushed", redisOps.flushDekCache());
                }

                // 9. After-snapshot; warn if old-version entries remain.
                if (oldVersion != null) {
                    int remaining = redisOps.countByVersion().getOrDefault(oldVersion, 0);
                    if (remaining > 0) {
                        log.warn("old_version_entries_remaining old_version={} remaining={}", oldVersion, remaining);
                    }
                }
            } catch (Exception e) {
                log.warn("post-rotation redis ops failed: {}", e.getMessage());
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("slot", inactiveSlot);
        result.put("kv_version", kvVersion);
        result.put("rotated_at", OffsetDateTime.now().toString());
        result.put("redis_ops", redisOpsResult);

        long elapsedMs = System.currentTimeMillis() - start;
        log.info("cek_rotation_complete slot={} kv_version={} elapsed_ms={}", inactiveSlot, kvVersion, elapsedMs);

        return result;
    }

    private String slotSecretName(String slot) {
        return "alpha".equals(slot) ? config.cekAlphaSecretName() : config.cekBetaSecretName();
    }

    private static String lastPathSegment(String uri) {
        String trimmed = uri.endsWith("/") ? uri.substring(0, uri.length() - 1) : uri;
        return trimmed.substring(trimmed.lastIndexOf('/') + 1);
    }
}
