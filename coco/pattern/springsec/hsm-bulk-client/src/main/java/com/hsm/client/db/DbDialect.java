package com.hsm.client.db;

/**
 * The three database engines DbBulkJob's checkpoint SQL is written for. Deliberately
 * not a general-purpose "any JDBC database" abstraction -- exactly the three engines
 * this module has real SQL generation logic for (see CheckpointStore), nothing more.
 *
 * <p>Resolution is JDBC-URL-scheme-first, with an explicit per-connection override
 * (ClientProperties.Db.TableRef.dialect) always winning when set. The URL scheme
 * identifies which wire protocol/driver is in use, not necessarily which real SQL
 * dialect quirks the backend has -- some products present a different vendor's wire
 * protocol for driver compatibility while diverging from that vendor's actual SQL
 * behavior (the reason the override exists at all, not just auto-detection).
 */
public enum DbDialect {
    POSTGRESQL,
    SQL_SERVER,
    ORACLE;

    /** Falls back to POSTGRESQL for any URL that isn't recognizably SQL Server/Oracle -- preserves this module's pre-existing Postgres-only behavior for every URL shape that isn't one of the two new dialects. */
    static DbDialect detect(String jdbcUrl) {
        if (jdbcUrl != null) {
            if (jdbcUrl.startsWith("jdbc:sqlserver:")) {
                return SQL_SERVER;
            }
            if (jdbcUrl.startsWith("jdbc:oracle:")) {
                return ORACLE;
            }
        }
        return POSTGRESQL;
    }

    /** override, when non-null, always wins over URL-scheme detection. */
    static DbDialect resolve(String jdbcUrl, DbDialect override) {
        return override != null ? override : detect(jdbcUrl);
    }
}
