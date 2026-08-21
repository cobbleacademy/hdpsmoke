package com.hsm.client.db;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Optional;

/**
 * Persists DB bulk job progress (the last successfully committed key-column value,
 * as text) so a crashed or killed run can resume past what was already written
 * instead of restarting from the first row.
 *
 * <p>{@link #save} must be called from within the same Spring transaction as the
 * data batch it accounts for -- this class relies entirely on {@link JdbcTemplate}
 * automatically participating in the caller's active transaction (both share the
 * same target {@code DataSource}), not on anything transactional here itself. That
 * coupling is what makes resume safe without requiring an upsert on the actual
 * target table: a batch's rows and its checkpoint update commit together or not
 * at all, so a resumed run can never skip past rows that were never durably written,
 * nor re-process rows that were.
 *
 * <p>Dialect-sensitive: table existence, DDL, and the upsert all differ genuinely
 * across Postgres/SQL Server/Oracle -- see the per-method comments below. Always
 * operates against the TARGET connection (checkpoint state always lives in the
 * target database, never source), so only one {@link DbDialect} is needed here,
 * not one per side.
 */
class CheckpointStore {

    private final String tableName;
    private final DbDialect dialect;

    CheckpointStore(String tableName, DbDialect dialect) {
        this.tableName = tableName;
        this.dialect = dialect;
    }

    /**
     * Checks existence first, and only issues CREATE TABLE when it's genuinely
     * missing -- deliberately not a blind "CREATE TABLE IF NOT EXISTS" (which isn't
     * even valid syntax on SQL Server, and only landed in Oracle in the very recent
     * 23c/23ai, long after most real deployments). Postgres additionally checks the
     * CREATE privilege on the schema *before* checking whether the table already
     * exists, so IF NOT EXISTS does not, by itself, avoid a permission error on a
     * table a DBA already pre-created -- a role can easily have full
     * INSERT/SELECT/UPDATE/DELETE on this exact table (granted directly,
     * object-level) without ever having schema-level CREATE, which is a completely
     * separate privilege and commonly withheld from application roles.
     */
    void ensureTable(JdbcTemplate jdbc) {
        if (tableExists(jdbc)) {
            return;
        }
        jdbc.execute(createTableDdl());
    }

    private boolean tableExists(JdbcTemplate jdbc) {
        return switch (dialect) {
            // to_regclass only needs the role to be able to see the table in the
            // catalog (implied by already having any privilege on it), never CREATE --
            // exactly the property that makes it safe against the Postgres
            // CREATE-privilege quirk described above.
            case POSTGRESQL -> Boolean.TRUE.equals(
                    jdbc.queryForObject("SELECT to_regclass(?) IS NOT NULL", Boolean.class, tableName));
            // OBJECT_ID(name, 'U') returns the object id of a user table if it exists,
            // NULL otherwise -- the standard T-SQL existence idiom. T-SQL has no native
            // boolean SELECT-list expression (comparisons are only valid in
            // WHERE/CASE/etc, not as a directly selected value pre-2022), hence the
            // explicit CASE/BIT wrapping.
            case SQL_SERVER -> Boolean.TRUE.equals(jdbc.queryForObject(
                    "SELECT CASE WHEN OBJECT_ID(?, N'U') IS NOT NULL THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END",
                    Boolean.class, tableName));
            // Oracle has no single catalog function equivalent to to_regclass/OBJECT_ID
            // that accepts a possibly-schema-qualified name as one string argument --
            // ALL_TABLES needs owner/table_name as separate predicates, which would
            // require parsing/quoting the qualified name ourselves. A trivial
            // zero-row SELECT against the table itself, treating any failure as
            // non-existence, is the standard, widely-used Oracle idiom for exactly
            // this reason. A real permission problem (rather than genuine
            // non-existence) degrades gracefully here too: it's misread as
            // "doesn't exist," then the CREATE TABLE below fails with a clear
            // insufficient-privileges error instead of this check's own exception
            // being surfaced directly -- an acceptable tradeoff for avoiding
            // qualified-name parsing entirely.
            case ORACLE -> {
                try {
                    jdbc.execute("SELECT 1 FROM " + tableName + " WHERE 1 = 0");
                    yield true;
                } catch (DataAccessException e) {
                    yield false;
                }
            }
        };
    }

    private String createTableDdl() {
        return switch (dialect) {
            case POSTGRESQL -> "CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                    "job_id VARCHAR(256) PRIMARY KEY, " +
                    "last_key VARCHAR(512), " +
                    "rows_done BIGINT NOT NULL DEFAULT 0, " +
                    "updated_at TIMESTAMPTZ NOT NULL DEFAULT now())";
            case SQL_SERVER -> "CREATE TABLE " + tableName + " (" +
                    "job_id VARCHAR(256) PRIMARY KEY, " +
                    "last_key VARCHAR(512), " +
                    "rows_done BIGINT NOT NULL DEFAULT 0, " +
                    "updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME())";
            // VARCHAR2 not VARCHAR (VARCHAR is a reserved-for-future-use synonym in
            // Oracle, not a real type to build new DDL against); NUMBER(19,0) is
            // Oracle's BIGINT-equivalent (no native 64-bit integer type); TIMESTAMP
            // (not TIMESTAMPTZ/TIMESTAMP WITH TIME ZONE) is the closest simple
            // equivalent for a purely informational "when was this last touched"
            // column, not used in any cross-timezone reconciliation logic elsewhere.
            case ORACLE -> "CREATE TABLE " + tableName + " (" +
                    "job_id VARCHAR2(256) PRIMARY KEY, " +
                    "last_key VARCHAR2(512), " +
                    "rows_done NUMBER(19,0) DEFAULT 0 NOT NULL, " +
                    "updated_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL)";
        };
    }

    Optional<String> loadLastKey(JdbcTemplate jdbc, String jobId) {
        List<String> found = jdbc.query(
                "SELECT last_key FROM " + tableName + " WHERE job_id = ?",
                (rs, rowNum) -> rs.getString("last_key"), jobId);
        return found.isEmpty() ? Optional.empty() : Optional.ofNullable(found.get(0));
    }

    /**
     * Must be called within the same transaction as the data batch it accounts for.
     * Postgres uses its native ON CONFLICT; SQL Server and Oracle have no such
     * shorthand and need a full MERGE -- both built with the target row's three
     * values aliased as named columns in the USING subquery (job_id/last_key/
     * rows_done), so each is bound exactly once and referenced via src.* in both the
     * WHEN MATCHED and WHEN NOT MATCHED branches, rather than needing every value
     * re-bound per branch. SQL Server's MERGE additionally requires a terminating
     * semicolon -- omitting it is a documented, explicit T-SQL error (10713), not a
     * style choice.
     */
    void save(JdbcTemplate jdbc, String jobId, Object lastKey, long rowsDone) {
        String lastKeyStr = String.valueOf(lastKey);
        switch (dialect) {
            case POSTGRESQL -> jdbc.update(
                    "INSERT INTO " + tableName + " (job_id, last_key, rows_done, updated_at) VALUES (?, ?, ?, now()) " +
                            "ON CONFLICT (job_id) DO UPDATE SET last_key = EXCLUDED.last_key, " +
                            "rows_done = EXCLUDED.rows_done, updated_at = now()",
                    jobId, lastKeyStr, rowsDone);
            case SQL_SERVER -> jdbc.update(
                    "MERGE INTO " + tableName + " AS tgt " +
                            "USING (SELECT ? AS job_id, ? AS last_key, ? AS rows_done) AS src " +
                            "ON tgt.job_id = src.job_id " +
                            "WHEN MATCHED THEN UPDATE SET last_key = src.last_key, rows_done = src.rows_done, updated_at = SYSUTCDATETIME() " +
                            "WHEN NOT MATCHED THEN INSERT (job_id, last_key, rows_done, updated_at) " +
                            "VALUES (src.job_id, src.last_key, src.rows_done, SYSUTCDATETIME());",
                    jobId, lastKeyStr, rowsDone);
            case ORACLE -> jdbc.update(
                    "MERGE INTO " + tableName + " tgt " +
                            "USING (SELECT ? AS job_id, ? AS last_key, ? AS rows_done FROM dual) src " +
                            "ON (tgt.job_id = src.job_id) " +
                            "WHEN MATCHED THEN UPDATE SET last_key = src.last_key, rows_done = src.rows_done, updated_at = SYSTIMESTAMP " +
                            "WHEN NOT MATCHED THEN INSERT (job_id, last_key, rows_done, updated_at) " +
                            "VALUES (src.job_id, src.last_key, src.rows_done, SYSTIMESTAMP)",
                    jobId, lastKeyStr, rowsDone);
        }
    }

    /** Used for resume=false ("override") -- clears prior progress before a fresh start. Plain DELETE, portable across all three dialects unchanged. */
    void clear(JdbcTemplate jdbc, String jobId) {
        jdbc.update("DELETE FROM " + tableName + " WHERE job_id = ?", jobId);
    }
}
