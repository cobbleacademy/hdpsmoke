package com.hsm.client.db;

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
 */
class CheckpointStore {

    private final String tableName;

    CheckpointStore(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Checks existence via to_regclass first, and only issues CREATE TABLE when it's
     * genuinely missing -- deliberately not a blind "CREATE TABLE IF NOT EXISTS".
     * Postgres checks the CREATE privilege on the schema *before* checking whether
     * the table already exists, so IF NOT EXISTS does not, by itself, avoid a
     * permission error on a table a DBA already pre-created -- a role can easily
     * have full INSERT/SELECT/UPDATE/DELETE on this exact table (granted directly,
     * object-level) without ever having schema-level CREATE, which is a completely
     * separate privilege and commonly withheld from application roles. to_regclass
     * only needs the role to be able to see the table in the catalog (implied by
     * already having any privilege on it), never CREATE.
     */
    void ensureTable(JdbcTemplate jdbc) {
        Boolean exists = jdbc.queryForObject("SELECT to_regclass(?) IS NOT NULL", Boolean.class, tableName);
        if (Boolean.TRUE.equals(exists)) {
            return;
        }
        jdbc.execute("CREATE TABLE IF NOT EXISTS " + tableName + " (" +
                "job_id VARCHAR(256) PRIMARY KEY, " +
                "last_key VARCHAR(512), " +
                "rows_done BIGINT NOT NULL DEFAULT 0, " +
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT now())");
    }

    Optional<String> loadLastKey(JdbcTemplate jdbc, String jobId) {
        List<String> found = jdbc.query(
                "SELECT last_key FROM " + tableName + " WHERE job_id = ?",
                (rs, rowNum) -> rs.getString("last_key"), jobId);
        return found.isEmpty() ? Optional.empty() : Optional.ofNullable(found.get(0));
    }

    /** Must be called within the same transaction as the data batch it accounts for. */
    void save(JdbcTemplate jdbc, String jobId, Object lastKey, long rowsDone) {
        jdbc.update(
                "INSERT INTO " + tableName + " (job_id, last_key, rows_done, updated_at) VALUES (?, ?, ?, now()) " +
                        "ON CONFLICT (job_id) DO UPDATE SET last_key = EXCLUDED.last_key, " +
                        "rows_done = EXCLUDED.rows_done, updated_at = now()",
                jobId, String.valueOf(lastKey), rowsDone);
    }

    /** Used for resume=false ("override") -- clears prior progress before a fresh start. */
    void clear(JdbcTemplate jdbc, String jobId) {
        jdbc.update("DELETE FROM " + tableName + " WHERE job_id = ?", jobId);
    }
}
