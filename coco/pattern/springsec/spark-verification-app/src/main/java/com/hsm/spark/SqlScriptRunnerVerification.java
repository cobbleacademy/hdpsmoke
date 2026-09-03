package com.hsm.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Path;

/**
 * Manual verification for HsmSqlScriptRunner -- runs a real ;;;-delimited SQL
 * script (CREATE TABLE ... USING org.apache.spark.sql.jdbc, then CREATE OR
 * REPLACE VIEW ... hsm_decrypt(...)) against a real JDBC source and a real
 * hsm-core-service instance, then queries the resulting view and checks the
 * decrypted value.
 *
 * <pre>
 * Required env vars: same as LocalSparkSessionManualVerification
 * (HSM_BASE_URL, HSM_APP_ID, HSM_AUTH_MODE, HSM_PRIVATE_KEY_PATH, plus the
 * auth-mode-specific one), plus:
 *   HSM_SQL_SCRIPT   file path to the ;;;-delimited SQL script to run
 * </pre>
 */
public final class SqlScriptRunnerVerification {

    private SqlScriptRunnerVerification() {
    }

    public static void main(String[] args) throws Exception {
        SparkSession.Builder builder = SparkSession.builder()
                .appName("hsm-sql-script-runner-verification")
                .master("local[*]")
                .config("spark.sql.extensions", "com.hsm.spark.HsmUdfExtension")
                .config("spark.hsm.baseUrl", requireEnv("HSM_BASE_URL"))
                .config("spark.hsm.appId", requireEnv("HSM_APP_ID"))
                .config("spark.hsm.authMode", requireEnv("HSM_AUTH_MODE"))
                .config("spark.hsm.privateKeyPath", requireEnv("HSM_PRIVATE_KEY_PATH"));

        switch (requireEnv("HSM_AUTH_MODE").trim().toUpperCase()) {
            case "STATIC" -> builder.config("spark.hsm.staticToken", requireEnv("HSM_STATIC_TOKEN"));
            case "AZURE_AD" -> builder.config("spark.hsm.azureTokenScope", requireEnv("HSM_AZURE_TOKEN_SCOPE"));
            case "SELF_SIGNED_JWT" -> builder.config("spark.hsm.signingKeyPath", requireEnv("HSM_SIGNING_KEY_PATH"));
            case "MTLS" -> builder
                    .config("spark.hsm.mtlsCertPath", requireEnv("HSM_MTLS_CERT_PATH"))
                    .config("spark.hsm.mtlsKeyPath", requireEnv("HSM_MTLS_KEY_PATH"));
            default -> throw new IllegalStateException("HSM_AUTH_MODE must be one of STATIC, AZURE_AD, SELF_SIGNED_JWT, MTLS");
        }

        try (SparkSession spark = builder.getOrCreate()) {
            Path scriptPath = Path.of(requireEnv("HSM_SQL_SCRIPT"));
            System.out.println("Running SQL script: " + scriptPath);
            HsmSqlScriptRunner.run(spark, scriptPath);

            Dataset<Row> result = spark.sql("SELECT * FROM customer_plain");
            result.show(false);

            String decryptedSsn = result.collectAsList().get(0).getAs("ssn");
            System.out.println("Decrypted ssn -> " + decryptedSsn);
            if (!"123-45-6789".equals(decryptedSsn)) {
                throw new IllegalStateException("Unexpected decrypted value: " + decryptedSsn);
            }
            System.out.println("View-over-JDBC-source verified: hsm_decrypt applied correctly through customer_plain.");
        }
    }

    private static String requireEnv(String envVar) {
        String value = System.getenv(envVar);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required env var: " + envVar);
        }
        return value;
    }
}
