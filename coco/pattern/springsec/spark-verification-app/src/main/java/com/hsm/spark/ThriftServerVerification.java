package com.hsm.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Manual verification for HsmThriftServerBootstrap -- starts a real Spark
 * Thrift Server (via HiveThriftServer2.startWithSparkSession, the same call
 * HsmThriftServerBootstrap itself makes) with hsm_decrypt already registered
 * and a decrypted view already created by HsmSqlScriptRunner, then connects
 * to it as a genuine JDBC client (org.apache.hive.jdbc.HiveDriver) and runs
 * a real query over the Thrift/JDBC wire protocol -- not just checking that
 * the server process starts without throwing.
 */
public final class ThriftServerVerification {

    private static final int THRIFT_PORT = 10321;

    private ThriftServerVerification() {
    }

    public static void main(String[] args) throws Exception {
        SparkSession.Builder builder = SparkSession.builder()
                .appName("hsm-thrift-server-verification")
                .master("local[*]")
                .config("spark.sql.extensions", "com.hsm.spark.HsmUdfExtension")
                .config("hive.server2.thrift.port", String.valueOf(THRIFT_PORT))
                .config("spark.hsm.baseUrl", requireEnv("HSM_BASE_URL"))
                .config("spark.hsm.appId", requireEnv("HSM_APP_ID"))
                .config("spark.hsm.authMode", requireEnv("HSM_AUTH_MODE"))
                .config("spark.hsm.privateKeyPath", requireEnv("HSM_PRIVATE_KEY_PATH"))
                .config("spark.hsm.signingKeyPath", requireEnv("HSM_SIGNING_KEY_PATH"));

        SparkSession spark = builder.getOrCreate();
        HsmSqlScriptRunner.run(spark, Path.of(requireEnv("HSM_SQL_SCRIPT")));

        System.out.println("Starting Thrift Server on port " + THRIFT_PORT + " ...");
        HiveThriftServer2 server = HiveThriftServer2.startWithSparkSession(spark, true);
        try {
            Thread.sleep(5000); // let the Thrift listener actually bind before connecting

            Class.forName("org.apache.hive.jdbc.HiveDriver");
            String url = "jdbc:hive2://localhost:" + THRIFT_PORT + "/default";
            System.out.println("Connecting as a real JDBC client: " + url);
            try (Connection conn = DriverManager.getConnection(url, "sa", "");
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT id, ssn, zip, address, phone FROM customer_plain")) {
                if (!rs.next()) {
                    throw new IllegalStateException("customer_plain returned no rows");
                }
                String ssn = rs.getString("ssn");
                System.out.println("Decrypted ssn (via real JDBC/Thrift connection) -> " + ssn);
                if (!"123-45-6789".equals(ssn)) {
                    throw new IllegalStateException("Unexpected decrypted value: " + ssn);
                }
                System.out.println("Thrift Server verified: real JDBC client, real Thrift wire protocol, "
                        + "hsm_decrypt applied correctly through customer_plain.");
            }
        } finally {
            server.stop();
            spark.stop();
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
