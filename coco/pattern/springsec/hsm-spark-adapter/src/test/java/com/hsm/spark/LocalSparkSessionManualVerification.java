package com.hsm.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Manual, IDE-run verification for hsm_encrypt/hsm_decrypt against a real
 * hsm-core-service instance -- no standalone cluster, no spark-submit. A
 * plain {@code master("local[*]")} SparkSession runs entirely in-process in
 * this one JVM, which is how this module's own automatic-registration path
 * (spark.sql.extensions=com.hsm.spark.HsmUdfExtension, the exact mechanism a
 * real deployment uses) was originally verified during development. Not a
 * JUnit test -- it needs a live hsm-core-service and real key material, so
 * it's a plain main() you run directly from your IDE (Run/Debug on this
 * file), not part of `mvn test`.
 *
 * <p>All connection/credential details come from environment variables --
 * nothing here is hardcoded, so this file never needs editing per-run or
 * per-environment. Only HSM_PRIVATE_KEY_PATH (DEK-transport unwrap key) is
 * always required; the rest depend on HSM_AUTH_MODE, mirroring
 * HsmSparkConfig's own spark.hsm.* conf requirements exactly -- see
 * SPARK_ADAPTER.md's Configuration table for what each one does.
 *
 * <pre>
 * Required always:
 *   HSM_BASE_URL           e.g. http://localhost:3005 (or https://... for MTLS)
 *   HSM_APP_ID              e.g. payments-svc
 *   HSM_AUTH_MODE            STATIC | AZURE_AD | SELF_SIGNED_JWT | MTLS
 *   HSM_PRIVATE_KEY_PATH     file path to the DEK-transport private key PEM
 *
 * One of, depending on HSM_AUTH_MODE:
 *   HSM_STATIC_TOKEN                  (STATIC)
 *   HSM_AZURE_TOKEN_SCOPE              (AZURE_AD)
 *   HSM_SIGNING_KEY_PATH                (SELF_SIGNED_JWT -- file path)
 *   HSM_MTLS_CERT_PATH, HSM_MTLS_KEY_PATH  (MTLS -- file paths)
 * </pre>
 */
public final class LocalSparkSessionManualVerification {

    private LocalSparkSessionManualVerification() {
    }

    public static void main(String[] args) throws Exception {
        SparkSession.Builder builder = SparkSession.builder()
                .appName("hsm-spark-adapter-local-verification")
                .master("local[*]")
                .config("spark.sql.extensions", "com.hsm.spark.HsmUdfExtension")
                .config("spark.hsm.baseUrl", require("HSM_BASE_URL"))
                .config("spark.hsm.appId", require("HSM_APP_ID"))
                .config("spark.hsm.authMode", require("HSM_AUTH_MODE"))
                .config("spark.hsm.privateKeyPath", require("HSM_PRIVATE_KEY_PATH"));

        switch (require("HSM_AUTH_MODE").trim().toUpperCase()) {
            case "STATIC" -> builder.config("spark.hsm.staticToken", require("HSM_STATIC_TOKEN"));
            case "AZURE_AD" -> builder.config("spark.hsm.azureTokenScope", require("HSM_AZURE_TOKEN_SCOPE"));
            case "SELF_SIGNED_JWT" -> builder.config("spark.hsm.signingKeyPath", require("HSM_SIGNING_KEY_PATH"));
            case "MTLS" -> builder
                    .config("spark.hsm.mtlsCertPath", require("HSM_MTLS_CERT_PATH"))
                    .config("spark.hsm.mtlsKeyPath", require("HSM_MTLS_KEY_PATH"));
            default -> throw new IllegalStateException("HSM_AUTH_MODE must be one of STATIC, AZURE_AD, SELF_SIGNED_JWT, MTLS");
        }

        try (SparkSession spark = builder.getOrCreate()) {
            System.out.println("SparkSession up, local[*], hsm_encrypt/hsm_decrypt registered via HsmUdfExtension.");

            Dataset<Row> encrypted = spark.sql(
                    "SELECT hsm_encrypt('hello from local IDE run') AS ciphertext");
            encrypted.show(false);
            String ciphertext = encrypted.collectAsList().get(0).getString(0);
            System.out.println("Encrypted -> " + ciphertext);

            Dataset<Row> decrypted = spark.sql(
                    "SELECT hsm_decrypt('" + ciphertext + "') AS plaintext");
            decrypted.show(false);
            String plaintext = decrypted.collectAsList().get(0).getString(0);

            if (!"hello from local IDE run".equals(plaintext)) {
                throw new IllegalStateException("Round-trip mismatch -- got: " + plaintext);
            }
            System.out.println("Round-trip verified: encrypt -> decrypt returned the original plaintext.");
        }
    }

    private static String require(String envVar) {
        String value = System.getenv(envVar);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required env var: " + envVar);
        }
        return value;
    }
}
