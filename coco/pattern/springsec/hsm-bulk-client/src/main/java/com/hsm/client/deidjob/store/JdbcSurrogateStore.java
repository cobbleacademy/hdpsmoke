package com.hsm.client.deidjob.store;

import com.hsm.client.db.DbDialect;
import com.hsm.client.deid.SurrogateStore;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The surrogate table, {@code deid_surrogate}, in the data owner's database
 * (DEIDENTIFICATION.md section 8). Created by the owner from
 * {@code deid/deid-surrogate-<dialect>.sql}; this class never creates or deletes
 * tables or rows.
 *
 * <p>Plain reads and inserts only; the algorithm is in {@code SurrogateTransform}.
 * {@link #insertIfAbsent} is a plain INSERT: when it breaks the primary key or the
 * unique surrogate constraint, the database refuses it and a read says which one,
 * which is portable across Postgres, SQL Server, Oracle and H2. Runs in autocommit
 * mode, so a refused insert never aborts a surrounding transaction. The database's own
 * message is not passed on: it can quote the surrogate.
 *
 * <p>{@code key_edek_id} is bound as a UUID on Postgres and H2 ({@code uuid} column)
 * and as its text on SQL Server ({@code uniqueidentifier}) and Oracle
 * ({@code VARCHAR2(36)}).
 */
public final class JdbcSurrogateStore implements SurrogateStore {

    /** Hashes per {@code IN (...)} read: under Oracle's 1000-item and SQL Server's 2100-parameter limits. */
    public static final int FIND_CHUNK = 500;

    private final JdbcTemplate jdbc;
    private final DbDialect dialect;
    private final String table;
    private final Clock clock;

    public JdbcSurrogateStore(JdbcTemplate jdbc, DbDialect dialect, String schema, String table, Clock clock) {
        if (!table.matches("[A-Za-z_][A-Za-z0-9_]*") || (schema != null && !schema.isBlank()
                && !schema.matches("[A-Za-z_][A-Za-z0-9_]*"))) {
            throw new IllegalArgumentException("deid.surrogate.schema/table must be plain identifiers");
        }
        this.jdbc = jdbc;
        this.dialect = dialect;
        this.table = schema == null || schema.isBlank() ? table : schema + "." + table;
        this.clock = clock;
    }

    @Override
    public void verifyAvailable() {
        try {
            jdbc.query("SELECT scope, domain, lookup_hash, value_enc, key_edek_id, surrogate, created_at, created_run_id FROM "
                    + table + " WHERE 1 = 0", rs -> null);
        } catch (DataAccessException e) {
            throw new IllegalStateException("the surrogate table " + table + " is missing or unusable; the owner's DBA "
                    + "creates it from " + ddlResource(dialect) + " (the job never creates tables)", e);
        }
    }

    @Override
    public Map<String, String> find(String scope, String domain, List<byte[]> lookupHashes) {
        if (lookupHashes.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> found = new HashMap<>();
        HexFormat hex = HexFormat.of();
        for (int from = 0; from < lookupHashes.size(); from += FIND_CHUNK) {
            List<byte[]> chunk = lookupHashes.subList(from, Math.min(lookupHashes.size(), from + FIND_CHUNK));
            String in = String.join(", ", Collections.nCopies(chunk.size(), "?"));
            List<Object> args = new ArrayList<>(chunk.size() + 2);
            args.add(scope);
            args.add(domain);
            args.addAll(chunk);
            jdbc.query("SELECT lookup_hash, surrogate FROM " + table + " WHERE scope = ? AND domain = ? AND lookup_hash IN ("
                    + in + ")", rs -> {
                found.put(hex.formatHex(rs.getBytes(1)), rs.getString(2));
            }, args.toArray());
        }
        return found;
    }

    @Override
    public InsertResult insertIfAbsent(String scope, String domain, byte[] lookupHash, byte[] valueEnc, UUID keyEdekId,
                                       String surrogate, String runId) {
        try {
            jdbc.update("INSERT INTO " + table + " (scope, domain, lookup_hash, value_enc, key_edek_id, surrogate, created_at, "
                            + "created_run_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    scope, domain, lookupHash, valueEnc, bindUuid(keyEdekId), surrogate,
                    Timestamp.from(clock.instant()), runId);
            return InsertResult.INSERTED;
        } catch (DataIntegrityViolationException e) {
            Integer rows = jdbc.queryForObject("SELECT COUNT(*) FROM " + table
                    + " WHERE scope = ? AND domain = ? AND lookup_hash = ?", Integer.class, scope, domain, lookupHash);
            if (rows != null && rows > 0) {
                return InsertResult.ROW_EXISTS;
            }
            Integer taken = jdbc.queryForObject("SELECT COUNT(*) FROM " + table
                    + " WHERE scope = ? AND domain = ? AND surrogate = ?", Integer.class, scope, domain, surrogate);
            if (taken != null && taken > 0) {
                return InsertResult.SURROGATE_TAKEN;
            }
            throw new IllegalStateException("insert into " + table + " was refused, and not by its primary key or "
                    + "surrogate constraint: " + e.getMostSpecificCause().getClass().getSimpleName());
        }
    }

    /** The shipped DDL for a dialect, on the classpath. */
    public static String ddlResource(DbDialect dialect) {
        return switch (dialect) {
            case POSTGRESQL -> "deid/deid-surrogate-postgres.sql";
            case SQL_SERVER -> "deid/deid-surrogate-sqlserver.sql";
            case ORACLE -> "deid/deid-surrogate-oracle.sql";
        };
    }

    private Object bindUuid(UUID id) {
        return dialect == DbDialect.POSTGRESQL ? id : id.toString();
    }
}
