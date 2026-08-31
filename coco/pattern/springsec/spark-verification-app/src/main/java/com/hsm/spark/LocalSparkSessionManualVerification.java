package com.hsm.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Local, no-cluster verification for hsm_encrypt/hsm_decrypt against a real
 * hsm-core-service instance. A plain {@code master("local[*]")} SparkSession
 * runs entirely in-process in this one JVM -- no spark-submit, no
 * start-master.sh/start-worker.sh. Registers via
 * {@code spark.sql.extensions=com.hsm.spark.HsmUdfExtension}, the exact
 * mechanism a real deployment uses (not the simpler
 * HsmUdfRegistration.registerAll(spark) explicit-registration alternative).
 *
 * <p>See this directory's README.md for setup (the two required jars) and
 * exactly how to run this class. Two ways to supply connection/credential
 * details -- nothing is hardcoded either way:
 *
 * <ol>
 *   <li><b>Environment variables</b> (default, used when HSM_CONF_FILE is
 *       unset) -- see the table below.</li>
 *   <li><b>A conf file</b> (set HSM_CONF_FILE to its path) -- the same
 *       {@code spark-defaults.conf} format a real cluster deployment uses
 *       ({@code spark.hsm.baseUrl http://...} or {@code spark.hsm.baseUrl=http://...}
 *       per line, {@code #}-prefixed comments allowed). Every {@code spark.*}
 *       key in the file is applied directly to the SparkSession builder --
 *       write it exactly as you would for spark-submit's own
 *       {@code --properties-file}/{@code conf/spark-defaults.conf}, and that
 *       same file works unmodified in a real deployment later.</li>
 * </ol>
 *
 * <pre>
 * Required always:
 *   HSM_BASE_URL           e.g. http://localhost:3005 (or https://... for MTLS)
 *   HSM_APP_ID              e.g. payments-svc
 *   HSM_AUTH_MODE            STATIC | AZURE_AD | SELF_SIGNED_JWT | MTLS
 *   HSM_PRIVATE_KEY_PATH     file path to the DEK-transport private key PEM
 *
 * Optional:
 *   HSM_API_V1_PREFIX       default /api/sensec/hsm/v1, same as HsmSparkConfig's
 *                           own spark.hsm.apiV1Prefix default -- only set this if
 *                           the target hsm-core-service's hsm.service.api-v1-prefix
 *                           was itself overridden away from that default.
 *
 * One of, depending on HSM_AUTH_MODE:
 *   HSM_STATIC_TOKEN                  (STATIC)
 *   HSM_AZURE_TOKEN_SCOPE              (AZURE_AD)
 *   HSM_SIGNING_KEY_PATH                (SELF_SIGNED_JWT -- file path)
 *   HSM_MTLS_CERT_PATH, HSM_MTLS_KEY_PATH  (MTLS -- file paths)
 * </pre>
 */
public final class LocalSparkSessionManualVerification {

    private static final List<String> REQUIRED_CONF_KEYS = List.of(
            "spark.sql.extensions", "spark.hsm.baseUrl", "spark.hsm.appId",
            "spark.hsm.authMode", "spark.hsm.privateKeyPath");

    private LocalSparkSessionManualVerification() {
    }

    public static void main(String[] args) throws Exception {
        SparkSession.Builder builder = SparkSession.builder()
                .appName("hsm-spark-adapter-local-verification")
                .master("local[*]");

        String confFile = System.getenv("HSM_CONF_FILE");
        if (confFile != null && !confFile.isBlank()) {
            applyConfFile(builder, Path.of(confFile));
        } else {
            applyEnvVars(builder);
        }

        try (SparkSession spark = builder.getOrCreate()) {
            System.out.println("SparkSession up, local[*], hsm_encrypt/hsm_decrypt registered via HsmUdfExtension.");

            Dataset<Row> encrypted = spark.sql(
                    "SELECT hsm_encrypt('hello from local verification', 'verification.column', NULL) AS ciphertext");
            encrypted.show(false);
            String ciphertext = encrypted.collectAsList().get(0).getString(0);
            System.out.println("Encrypted -> " + ciphertext);

            Dataset<Row> decrypted = spark.sql(
                    "SELECT hsm_decrypt('" + ciphertext + "') AS plaintext");
            decrypted.show(false);
            String plaintext = decrypted.collectAsList().get(0).getString(0);

            if (!"hello from local verification".equals(plaintext)) {
                throw new IllegalStateException("Round-trip mismatch -- got: " + plaintext);
            }
            System.out.println("Round-trip verified: encrypt -> decrypt returned the original plaintext.");
        }
    }

    private static void applyEnvVars(SparkSession.Builder builder) {
        builder.config("spark.sql.extensions", "com.hsm.spark.HsmUdfExtension")
                .config("spark.hsm.baseUrl", requireEnv("HSM_BASE_URL"))
                .config("spark.hsm.appId", requireEnv("HSM_APP_ID"))
                .config("spark.hsm.authMode", requireEnv("HSM_AUTH_MODE"))
                .config("spark.hsm.privateKeyPath", requireEnv("HSM_PRIVATE_KEY_PATH"));

        // Optional -- HsmSparkConfig itself defaults spark.hsm.apiV1Prefix to
        // /api/sensec/hsm/v1 when unset, so only set this if the target
        // hsm-core-service's hsm.service.api-v1-prefix was itself overridden.
        String apiV1Prefix = System.getenv("HSM_API_V1_PREFIX");
        if (apiV1Prefix != null && !apiV1Prefix.isBlank()) {
            builder.config("spark.hsm.apiV1Prefix", apiV1Prefix);
        }

        switch (requireEnv("HSM_AUTH_MODE").trim().toUpperCase()) {
            case "STATIC" -> builder.config("spark.hsm.staticToken", requireEnv("HSM_STATIC_TOKEN"));
            case "AZURE_AD" -> builder.config("spark.hsm.azureTokenScope", requireEnv("HSM_AZURE_TOKEN_SCOPE"));
            case "SELF_SIGNED_JWT" -> builder.config("spark.hsm.signingKeyPath", requireEnv("HSM_SIGNING_KEY_PATH"));
            case "MTLS" -> builder
                    .config("spark.hsm.mtlsCertPath", requireEnv("HSM_MTLS_CERT_PATH"))
                    .config("spark.hsm.mtlsKeyPath", requireEnv("HSM_MTLS_KEY_PATH"));
            default -> throw new IllegalStateException("HSM_AUTH_MODE must be one of STATIC, AZURE_AD, SELF_SIGNED_JWT, MTLS");
        }
    }

    /** Parses the same spark-defaults.conf shape spark-submit's --properties-file uses
     * (key value, or key=value, one per line, # comments, blank lines ignored) and applies
     * every spark.* entry directly -- the file is portable, unmodified, to a real deployment. */
    private static void applyConfFile(SparkSession.Builder builder, Path path) throws IOException {
        Map<String, String> conf = new LinkedHashMap<>();
        for (String rawLine : Files.readAllLines(path)) {
            String line = rawLine.strip();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            String key;
            String value;
            int eq = line.indexOf('=');
            int space = line.indexOf(' ');
            int split = (eq == -1) ? space : (space == -1 ? eq : Math.min(eq, space));
            if (split == -1) {
                throw new IllegalStateException("Malformed conf line in " + path + " (expected 'key value' or 'key=value'): " + rawLine);
            }
            key = line.substring(0, split).strip();
            value = line.substring(split + 1).strip();
            if (value.startsWith("=")) {
                value = value.substring(1).strip();
            }
            conf.put(key, value);
        }

        for (String requiredKey : REQUIRED_CONF_KEYS) {
            if (!conf.containsKey(requiredKey)) {
                throw new IllegalStateException("Missing required key '" + requiredKey + "' in conf file " + path);
            }
        }
        conf.forEach(builder::config);
    }

    private static String requireEnv(String envVar) {
        String value = System.getenv(envVar);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required env var: " + envVar);
        }
        return value;
    }
}
