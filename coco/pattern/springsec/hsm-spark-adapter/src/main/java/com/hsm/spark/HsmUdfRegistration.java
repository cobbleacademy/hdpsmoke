package com.hsm.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * Explicit, per-application registration of {@code hsm_encrypt}/
 * {@code hsm_decrypt} -- built entirely on {@link SparkSession}'s public,
 * stable {@code udf().register(...)} API, unlike {@link HsmUdfExtension}
 * (which reaches into Catalyst-internal APIs to register automatically for
 * every application on a cluster). Use this directly in application code
 * when you don't control {@code spark.sql.extensions} cluster-wide config,
 * or want per-application control over exactly when registration happens:
 *
 * <pre>{@code
 * SparkSession spark = SparkSession.builder()...getOrCreate();
 * HsmUdfRegistration.registerAll(spark);
 * spark.sql("SELECT hsm_encrypt(ssn, 'customers.ssn', 'pii') FROM customers");
 * }</pre>
 *
 * <p>Both functions still resolve their connection/identity config
 * ({@code spark.hsm.*}) lazily per executor at first invocation (see
 * {@link HsmSparkConfig}), exactly as they do when registered via
 * {@link HsmUdfExtension} -- this class differs only in <i>when and how</i>
 * registration happens, not in the functions' own behavior.
 */
public final class HsmUdfRegistration {

    public static final String ENCRYPT_FUNCTION_NAME = "hsm_encrypt";
    public static final String DECRYPT_FUNCTION_NAME = "hsm_decrypt";

    private HsmUdfRegistration() {
    }

    public static void registerAll(SparkSession spark) {
        spark.udf().register(ENCRYPT_FUNCTION_NAME, new HsmEncryptUdf(), DataTypes.StringType);
        spark.udf().register(DECRYPT_FUNCTION_NAME, new HsmDecryptUdf(), DataTypes.StringType);
    }
}
