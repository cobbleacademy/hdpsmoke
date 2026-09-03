package com.hsm.spark;

import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Runs a sequence of plain Spark SQL statements from a file -- typically a
 * {@code CREATE TABLE ... USING org.apache.spark.sql.jdbc} defining a source
 * table, followed by a {@code CREATE OR REPLACE VIEW ... AS SELECT ...,
 * hsm_decrypt(col), ...} exposing a decrypted view over it, though this class
 * itself knows nothing about tables/views/decryption specifically -- it's a
 * generic multi-statement SQL script runner. The actual decrypt-view use case
 * comes entirely from what {@code hsm_decrypt}/{@code hsm_encrypt} already
 * being registered (via {@link HsmUdfExtension} or {@link HsmUdfRegistration})
 * lets ordinary SQL do; no crypto-specific logic lives here.
 *
 * <p>Statements are split on {@code ;;;}, not a plain {@code ;} -- a single
 * semicolon can legitimately appear inside a statement itself (a JDBC URL's
 * query string, a quoted string literal, a WHERE clause), so it can't double
 * as the statement separator. {@code ;;;} is chosen specifically because it's
 * not valid SQL syntax on its own, so it can never collide with real
 * statement content by accident.
 *
 * <p>Deliberately no JDBC driver dependency in this module's own pom.xml --
 * same reasoning hsm-bulk-client's own DB jobs already follow: the driver
 * (Postgres, SQL Server, Oracle, H2, ...) is whatever the actual source/target
 * database needs, decided at deploy time by whoever runs the job, not baked
 * into this adapter. Add it to the job's own classpath (e.g.
 * {@code --jars .../postgresql-....jar} alongside this module's shaded jar).
 */
public final class HsmSqlScriptRunner {

    private static final String STATEMENT_DELIMITER = ";;;";

    private HsmSqlScriptRunner() {
    }

    public static void run(SparkSession spark, Path scriptPath) throws IOException {
        String content = Files.readString(scriptPath, StandardCharsets.UTF_8);
        for (String rawStatement : content.split(STATEMENT_DELIMITER)) {
            String statement = rawStatement.strip();
            if (statement.isEmpty()) {
                continue;
            }
            spark.sql(statement);
        }
    }
}
