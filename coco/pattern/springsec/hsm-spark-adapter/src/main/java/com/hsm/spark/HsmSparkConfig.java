package com.hsm.spark;

import com.hsm.client.HsmCryptoClient;
import org.apache.spark.SparkEnv;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Reads {@code spark.hsm.*} Spark conf -- plus the key file(s) it points at --
 * into a built {@link HsmCryptoClient}. Read lazily, once per executor JVM (via
 * {@link HsmCryptoClientHolder}), from the <b>active application's own</b>
 * {@link SparkEnv} -- deliberately not captured at {@link HsmUdfExtension}
 * construction time. That's what keeps registration cluster-wide (every new
 * application gets {@code hsm_encrypt}/{@code hsm_decrypt} automatically) while
 * identity/credentials stay per-application: each application still supplies
 * its own {@code spark.hsm.appId} and key paths via its own {@code --conf} at
 * spark-submit time, and this class picks that up the first time a UDF
 * actually runs on one of that application's executors -- one identity per
 * application, shared by every job in it, same convention hsm-bulk-client's
 * own single-{@code app-id}-per-run config already uses.
 *
 * <p>Only {@code spark.hsm.baseUrl}, {@code spark.hsm.appId},
 * {@code spark.hsm.authMode}, and {@code spark.hsm.privateKeyPath} are always
 * required; the rest depend on {@code authMode}. {@code privateKeyPath} and
 * {@code signingKeyPath} are file paths (a Secret mounted identically on every
 * executor), never the key material itself -- putting raw key PEM into a
 * Spark conf value would put it on the driver-to-executor conf propagation
 * path, which this is deliberately avoiding.
 */
final class HsmSparkConfig {

    private HsmSparkConfig() {
    }

    static HsmCryptoClient buildClient() {
        String baseUrl = require("spark.hsm.baseUrl");
        String apiV1Prefix = optional("spark.hsm.apiV1Prefix", "/api/sensec/hsm/v1");
        String appId = require("spark.hsm.appId");
        String authMode = require("spark.hsm.authMode").trim().toUpperCase();
        String privateKeyPem = readFile(require("spark.hsm.privateKeyPath"));

        HsmCryptoClient.Builder builder = HsmCryptoClient.builder()
                .baseUrl(baseUrl)
                .apiV1Prefix(apiV1Prefix)
                .appId(appId)
                .privateKeyPem(privateKeyPem);

        switch (authMode) {
            case "STATIC" -> builder.staticToken(require("spark.hsm.staticToken"));
            case "AZURE_AD" -> builder.azureAdToken(require("spark.hsm.azureTokenScope"));
            case "SELF_SIGNED_JWT" -> {
                String signingKeyPem = readFile(require("spark.hsm.signingKeyPath"));
                String audience = optional("spark.hsm.selfSignedAudience", null);
                builder.selfSignedJwt(signingKeyPem, audience);
            }
            default -> throw new IllegalStateException(
                    "spark.hsm.authMode must be one of STATIC, AZURE_AD, SELF_SIGNED_JWT -- got: " + authMode);
        }

        return builder.build();
    }

    private static String require(String key) {
        String value = SparkEnv.get().conf().get(key, null);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "Missing required Spark conf '" + key + "' -- set it via --conf " + key + "=... at spark-submit time.");
        }
        return value;
    }

    private static String optional(String key, String defaultValue) {
        return SparkEnv.get().conf().get(key, defaultValue);
    }

    private static String readFile(String path) {
        try {
            return Files.readString(Path.of(path));
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to read key file at '" + path + "' -- confirm the Secret is mounted at this exact path on every executor.", e);
        }
    }
}
