package com.hsm.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2;

import java.nio.file.Path;

/**
 * Runs the Spark Thrift Server with hsm_encrypt/hsm_decrypt already registered
 * and any decrypted views already created, as one self-contained process --
 * not a separate {@code start-thriftserver.sh} plus a manual {@code beeline -f}
 * step run against it afterward. That two-step shape is what this class
 * replaces: config drives registration, a second, disconnected manual step
 * drives view creation, and nothing ties them together or guarantees the
 * views exist before the first client connects.
 *
 * <p>Built entirely on Spark's own public embedding API for the Thrift Server
 * ({@link HiveThriftServer2#startWithSparkSession(SparkSession, boolean)}) --
 * not a fork, not reflection into internals. That method starts serving the
 * Thrift/JDBC endpoint on top of an already-built {@link SparkSession}, which
 * is exactly the hook needed: build the session (registering the UDFs via
 * {@code spark.sql.extensions=com.hsm.spark.HsmUdfExtension}, same as any
 * other deployment), optionally run a startup SQL script (creating source
 * tables and decrypt views via {@link HsmSqlScriptRunner}), *then* start the
 * server -- so by the time any client can connect, the views already exist.
 *
 * <p>Deployed the same way as {@code org.apache.spark.sql.hive.thriftserver.HiveThriftServer2}
 * itself: {@code spark-submit --class com.hsm.spark.HsmThriftServerBootstrap
 * --master <yarn|k8s://...|spark://...> --jars hsm-spark-adapter-1.0.0.jar,bc-fips-2.1.1.jar,<jdbc-driver>.jar
 * hsm-spark-adapter-1.0.0.jar}. {@code spark-hive-thriftserver} itself is
 * {@code provided} scope (see this module's pom.xml) -- any real cluster
 * capable of running Thrift Server already ships it.
 *
 * <pre>
 * Required always: same as HsmSparkConfig's own spark.hsm.* keys (baseUrl,
 * appId, authMode, privateKeyPath, plus the auth-mode-specific one) -- see
 * SPARK_ADAPTER.md's Configuration table.
 *
 * Optional:
 *   spark.hsm.startupSqlScript   file path to a ;;;-delimited SQL script
 *                                  (CREATE TABLE ... USING org.apache.spark.sql.jdbc,
 *                                  CREATE OR REPLACE VIEW ... hsm_decrypt(...))
 *                                  run once, before the server starts accepting
 *                                  connections. Omit to start with no
 *                                  pre-created views (e.g. if beeline/JDBC
 *                                  clients will issue their own DDL later).
 * </pre>
 */
public final class HsmThriftServerBootstrap {

    private HsmThriftServerBootstrap() {
    }

    public static void main(String[] args) throws Exception {
        SparkSession.Builder builder = SparkSession.builder()
                .appName("hsm-thrift-server")
                .config("spark.sql.extensions", "com.hsm.spark.HsmUdfExtension");
        // Every other spark.hsm.* / spark.master / spark.jars config comes from
        // spark-defaults.conf or --conf flags at spark-submit time, same as any
        // other Spark application -- nothing else is set programmatically here,
        // deliberately, so this class behaves like a normal spark-submit target.

        try (SparkSession spark = builder.getOrCreate()) {
            String startupScript = spark.conf().get("spark.hsm.startupSqlScript", "");
            if (!startupScript.isBlank()) {
                HsmSqlScriptRunner.run(spark, Path.of(startupScript));
            }
            HiveThriftServer2.startWithSparkSession(spark, true);
            // startWithSparkSession runs the server on its own thread pool and
            // returns immediately -- block here so this process (and the
            // SparkSession/try-with-resources close it would trigger) stays
            // alive for the server's actual lifetime.
            Thread.currentThread().join();
        }
    }
}
