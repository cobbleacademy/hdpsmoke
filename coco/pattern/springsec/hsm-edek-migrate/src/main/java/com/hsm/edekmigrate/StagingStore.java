package com.hsm.edekmigrate;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.UnaryOperator;

/**
 * B's side: the staging table ({@code <staging schema>.edek_records}), and the promote
 * into B's {@code edek_records}. The caller owns transactions: the connection has
 * auto-commit off, and the caller commits once per page.
 *
 * <p>A staged row keeps A's EDEK ({@code source_edek_blob}, wrapped by A's KEK) next
 * to B's ({@code edek_blob}, wrapped by B's KEK, once {@code wrap} has run). Both are
 * ciphertext; the DEK itself is never stored, nor anything derived from it.
 */
public final class StagingStore {

    /** Every value {@code migrate_status} can hold. */
    public static final List<String> STATUSES = List.of("staged", "wrapped", "verified", "failed", "promoted",
            "removed");

    private final Connection db;
    private final String stagingSchema;
    private final String staging;
    private final String target;
    private final String targetAccess;

    public StagingStore(Connection db, String stagingSchema, String targetCryptoSchema, String targetAccessSchema) {
        this.db = db;
        this.stagingSchema = stagingSchema;
        this.staging = stagingSchema + ".edek_records";
        this.target = targetCryptoSchema + ".edek_records";
        this.targetAccess = targetAccessSchema;
    }

    /** Creates the staging schema and table if they don't exist yet. */
    public void init() throws SQLException {
        try (Statement st = db.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS " + stagingSchema);
            st.execute("CREATE TABLE IF NOT EXISTS " + staging + " ("
                    + "edek_id UUID PRIMARY KEY,"
                    + "app_id VARCHAR(128) NOT NULL,"
                    + "edek_blob TEXT,"
                    + "kek_version VARCHAR(64),"
                    + "kek_name VARCHAR(127) NOT NULL,"
                    + "algorithm VARCHAR(32) NOT NULL,"
                    + "encoding VARCHAR(16) NOT NULL,"
                    + "data_classification VARCHAR(32),"
                    + "rotation_status VARCHAR(16) NOT NULL,"
                    + "created_at TIMESTAMP WITH TIME ZONE,"
                    + "rotated_at TIMESTAMP WITH TIME ZONE,"
                    + "fingerprint VARCHAR(16),"
                    + "dek_name VARCHAR(256),"
                    + "current_dek_name VARCHAR(256),"
                    + "source_edek_blob TEXT NOT NULL,"
                    + "source_kek_name VARCHAR(127) NOT NULL,"
                    + "source_kek_version VARCHAR(64) NOT NULL,"
                    + "source_row_hash CHAR(64) NOT NULL,"
                    + "migrate_status VARCHAR(16) NOT NULL CHECK (migrate_status IN "
                    + "('staged','wrapped','verified','failed','promoted','removed')),"
                    + "error_type VARCHAR(200),"
                    + "batch_id VARCHAR(40) NOT NULL,"
                    + "seen_batch VARCHAR(40) NOT NULL,"
                    + "staged_at TIMESTAMP WITH TIME ZONE NOT NULL,"
                    + "wrapped_at TIMESTAMP WITH TIME ZONE,"
                    + "verified_at TIMESTAMP WITH TIME ZONE,"
                    + "promoted_at TIMESTAMP WITH TIME ZONE)");
            st.execute("CREATE INDEX IF NOT EXISTS edek_records_migrate_status ON " + staging
                    + " (migrate_status, edek_id)");
        }
    }

    // ---- stage ----

    public record StageCounts(int added, int changed, int unchanged) {
    }

    /**
     * Adds new rows as {@code staged}; puts rows whose source hash changed back to
     * {@code staged} (B's blob cleared, to be wrapped again); marks every row seen in
     * this batch. {@code targetKek} maps a source KEK name to B's.
     */
    public StageCounts stage(List<EdekRow> rows, String batch, String legacyKekName, UnaryOperator<String> targetKek)
            throws SQLException {
        Map<UUID, String> existing = existingHashes(rows);
        int added = 0;
        int changed = 0;
        int unchanged = 0;
        try (PreparedStatement insert = db.prepareStatement("INSERT INTO " + staging + " (edek_id, app_id, kek_name, "
                + "algorithm, encoding, data_classification, rotation_status, created_at, rotated_at, fingerprint, "
                + "dek_name, current_dek_name, source_edek_blob, source_kek_name, source_kek_version, "
                + "source_row_hash, migrate_status, batch_id, seen_batch, staged_at) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'staged',?,?,now())");
             PreparedStatement update = db.prepareStatement("UPDATE " + staging + " SET app_id=?, kek_name=?, "
                     + "algorithm=?, encoding=?, data_classification=?, rotation_status=?, created_at=?, rotated_at=?, "
                     + "fingerprint=?, dek_name=?, current_dek_name=?, source_edek_blob=?, source_kek_name=?, "
                     + "source_kek_version=?, source_row_hash=?, migrate_status='staged', edek_blob=NULL, "
                     + "kek_version=NULL, error_type=NULL, wrapped_at=NULL, verified_at=NULL, batch_id=?, "
                     + "seen_batch=?, staged_at=now() WHERE edek_id=?");
             PreparedStatement seen = db.prepareStatement("UPDATE " + staging
                     + " SET seen_batch=? WHERE edek_id=?")) {
            for (EdekRow r : rows) {
                String hash = r.sourceHash();
                String sourceKek = r.resolvedKekName(legacyKekName);
                String oldHash = existing.get(r.edekId());
                if (oldHash == null) {
                    int i = 1;
                    insert.setObject(i++, r.edekId());
                    insert.setString(i++, r.appId());
                    i = setCommon(insert, i, r, targetKek.apply(sourceKek), sourceKek, hash);
                    insert.setString(i++, batch);
                    insert.setString(i, batch);
                    insert.addBatch();
                    added++;
                } else if (!oldHash.equals(hash)) {
                    int i = 1;
                    update.setString(i++, r.appId());
                    i = setCommon(update, i, r, targetKek.apply(sourceKek), sourceKek, hash);
                    update.setString(i++, batch);
                    update.setString(i++, batch);
                    update.setObject(i, r.edekId());
                    update.addBatch();
                    changed++;
                } else {
                    seen.setString(1, batch);
                    seen.setObject(2, r.edekId());
                    seen.addBatch();
                    unchanged++;
                }
            }
            insert.executeBatch();
            update.executeBatch();
            seen.executeBatch();
        }
        return new StageCounts(added, changed, unchanged);
    }

    /** kek_name ... source_row_hash, in the order both stage statements use. */
    private static int setCommon(PreparedStatement ps, int i, EdekRow r, String targetKek, String sourceKek, String hash)
            throws SQLException {
        ps.setString(i++, targetKek);
        ps.setString(i++, r.algorithm());
        ps.setString(i++, r.encoding());
        ps.setString(i++, r.dataClassification());
        ps.setString(i++, r.rotationStatus());
        ps.setObject(i++, r.createdAt());
        ps.setObject(i++, r.rotatedAt());
        ps.setString(i++, r.fingerprint());
        ps.setString(i++, r.dekName());
        ps.setString(i++, r.currentDekName());
        ps.setString(i++, r.edekBlob());
        ps.setString(i++, sourceKek);
        ps.setString(i++, r.kekVersion());
        ps.setString(i++, hash);
        return i;
    }

    private Map<UUID, String> existingHashes(List<EdekRow> rows) throws SQLException {
        Map<UUID, String> out = new HashMap<>();
        if (rows.isEmpty()) {
            return out;
        }
        try (PreparedStatement ps = db.prepareStatement("SELECT edek_id, source_row_hash, migrate_status FROM "
                + staging + " WHERE edek_id = ANY(?)")) {
            ps.setArray(1, uuidArray(rows.stream().map(EdekRow::edekId).toList()));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // A row marked removed that is back in A is staged again, as if it had changed.
                    String hash = "removed".equals(rs.getString(3)) ? "" : rs.getString(2);
                    out.put(rs.getObject(1, UUID.class), hash);
                }
            }
        }
        return out;
    }

    /** Rows in scope that this batch didn't see in A any more: marked {@code removed}. */
    public int markRemoved(String batch, Scope scope) throws SQLException {
        StringBuilder sql = new StringBuilder("UPDATE " + staging
                + " SET migrate_status='removed', error_type=NULL WHERE seen_batch <> ?");
        List<Object> args = new ArrayList<>(List.of(batch));
        appendScope(sql, args, scope);
        try (PreparedStatement ps = prepare(sql.toString(), args)) {
            return ps.executeUpdate();
        }
    }

    // ---- wrap and verify ----

    public record StagedRow(UUID edekId, String appId, String sourceBlob, String sourceKekName,
                            String sourceKekVersion, String targetKekName, String targetBlob,
                            String targetKekVersion) {
    }

    public List<StagedRow> page(Set<String> statuses, UUID after, int limit, Scope scope) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT edek_id, app_id, source_edek_blob, source_kek_name, "
                + "source_kek_version, kek_name, edek_blob, kek_version FROM " + staging
                + " WHERE migrate_status = ANY(?)");
        List<Object> args = new ArrayList<>();
        args.add(db.createArrayOf("varchar", statuses.toArray()));
        if (after != null) {
            sql.append(" AND edek_id > ?");
            args.add(after);
        }
        appendScope(sql, args, scope);
        sql.append(" ORDER BY edek_id LIMIT ?");
        args.add(limit);
        try (PreparedStatement ps = prepare(sql.toString(), args); ResultSet rs = ps.executeQuery()) {
            List<StagedRow> out = new ArrayList<>();
            while (rs.next()) {
                out.add(new StagedRow(rs.getObject(1, UUID.class), rs.getString(2), rs.getString(3), rs.getString(4),
                        rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8)));
            }
            return out;
        }
    }

    public void setWrapped(UUID id, String blob, String kekVersion, String batch) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement("UPDATE " + staging + " SET migrate_status='wrapped', "
                + "edek_blob=?, kek_version=?, error_type=NULL, wrapped_at=now(), verified_at=NULL, batch_id=? "
                + "WHERE edek_id=?")) {
            ps.setString(1, blob);
            ps.setString(2, kekVersion);
            ps.setString(3, batch);
            ps.setObject(4, id);
            ps.executeUpdate();
        }
    }

    public void setVerified(UUID id, String batch) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement("UPDATE " + staging + " SET migrate_status='verified', "
                + "error_type=NULL, verified_at=now(), batch_id=? WHERE edek_id=?")) {
            ps.setString(1, batch);
            ps.setObject(2, id);
            ps.executeUpdate();
        }
    }

    public void setFailed(UUID id, String errorType, String batch) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement("UPDATE " + staging + " SET migrate_status='failed', "
                + "error_type=?, verified_at=NULL, batch_id=? WHERE edek_id=?")) {
            ps.setString(1, errorType.length() > 200 ? errorType.substring(0, 200) : errorType);
            ps.setString(2, batch);
            ps.setObject(3, id);
            ps.executeUpdate();
        }
    }

    // ---- report ----

    public record GroupCount(String sourceKekName, String appId, String status, long rows) {
    }

    public record Failed(UUID edekId, String sourceKekName, String appId, String errorType) {
    }

    public List<GroupCount> counts(Scope scope) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT source_kek_name, app_id, migrate_status, COUNT(*) FROM "
                + staging + " WHERE TRUE");
        List<Object> args = new ArrayList<>();
        appendScope(sql, args, scope);
        sql.append(" GROUP BY 1, 2, 3 ORDER BY 1, 2, 3");
        try (PreparedStatement ps = prepare(sql.toString(), args); ResultSet rs = ps.executeQuery()) {
            List<GroupCount> out = new ArrayList<>();
            while (rs.next()) {
                out.add(new GroupCount(rs.getString(1), rs.getString(2), rs.getString(3), rs.getLong(4)));
            }
            return out;
        }
    }

    public List<Failed> failed(Scope scope, int limit) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT edek_id, source_kek_name, app_id, error_type FROM " + staging
                + " WHERE migrate_status = 'failed'");
        List<Object> args = new ArrayList<>();
        appendScope(sql, args, scope);
        sql.append(" ORDER BY source_kek_name, edek_id LIMIT ?");
        args.add(limit);
        try (PreparedStatement ps = prepare(sql.toString(), args); ResultSet rs = ps.executeQuery()) {
            List<Failed> out = new ArrayList<>();
            while (rs.next()) {
                out.add(new Failed(rs.getObject(1, UUID.class), rs.getString(2), rs.getString(3), rs.getString(4)));
            }
            return out;
        }
    }

    // ---- promote ----

    public record PromoteResult(boolean refused, Map<String, Long> blockers, long clashes, int updated,
                                int inserted) {
    }

    /**
     * Copies {@code verified} rows into B's {@code edek_records}, in the caller's
     * transaction. Refused while the scope holds a row in any other state than
     * {@code verified} or {@code promoted}, or a verified row whose edek_id is already in
     * B's table but wasn't put there by an earlier promote (another row's ID).
     *
     * <p>Order inside the transaction: rows promoted earlier first give up a
     * current_dek_name they no longer hold (it's unique across all rows), then they're
     * updated, then new rows are inserted.
     */
    public PromoteResult promote(Scope scope) throws SQLException {
        Map<String, Long> blockers = new LinkedHashMap<>();
        StringBuilder sql = new StringBuilder("SELECT migrate_status, COUNT(*) FROM " + staging
                + " WHERE migrate_status NOT IN ('verified','promoted')");
        List<Object> args = new ArrayList<>();
        appendScope(sql, args, scope);
        sql.append(" GROUP BY 1 ORDER BY 1");
        try (PreparedStatement ps = prepare(sql.toString(), args); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                blockers.put(rs.getString(1), rs.getLong(2));
            }
        }

        StringBuilder clash = new StringBuilder("SELECT COUNT(*) FROM " + staging + " s JOIN " + target
                + " t ON t.edek_id = s.edek_id WHERE s.migrate_status = 'verified' AND s.promoted_at IS NULL");
        List<Object> clashArgs = new ArrayList<>();
        appendScope(clash, clashArgs, scope, "s.");
        long clashes;
        try (PreparedStatement ps = prepare(clash.toString(), clashArgs); ResultSet rs = ps.executeQuery()) {
            rs.next();
            clashes = rs.getLong(1);
        }
        if (!blockers.isEmpty() || clashes > 0) {
            return new PromoteResult(true, blockers, clashes, 0, 0);
        }

        String match = " FROM " + staging + " s WHERE s.edek_id = t.edek_id AND s.migrate_status = 'verified'";
        List<Object> scopeArgs = new ArrayList<>();
        StringBuilder scopeSql = new StringBuilder();
        appendScope(scopeSql, scopeArgs, scope, "s.");

        try (PreparedStatement ps = prepare("UPDATE " + target + " t SET current_dek_name = NULL" + match + scopeSql
                + " AND t.current_dek_name IS DISTINCT FROM s.current_dek_name", scopeArgs)) {
            ps.executeUpdate();
        }
        int updated;
        try (PreparedStatement ps = prepare("UPDATE " + target + " t SET app_id = s.app_id, edek_blob = s.edek_blob, "
                + "kek_version = s.kek_version, kek_name = s.kek_name, previous_kek_name = NULL, "
                + "previous_kek_version = NULL, previous_edek_blob = NULL, algorithm = s.algorithm, "
                + "encoding = s.encoding, data_classification = s.data_classification, "
                + "rotation_status = s.rotation_status, created_at = s.created_at, rotated_at = s.rotated_at, "
                + "fingerprint = s.fingerprint, dek_name = s.dek_name, current_dek_name = s.current_dek_name"
                + match + scopeSql, scopeArgs)) {
            updated = ps.executeUpdate();
        }
        int inserted;
        StringBuilder insert = new StringBuilder("INSERT INTO " + target + " (" + EdekRow.COLUMNS + ") SELECT "
                + "s.edek_id, s.app_id, s.edek_blob, s.kek_version, s.kek_name, NULL, NULL, NULL, s.algorithm, "
                + "s.encoding, s.data_classification, s.rotation_status, s.created_at, s.rotated_at, s.fingerprint, "
                + "s.dek_name, s.current_dek_name FROM " + staging + " s WHERE s.migrate_status = 'verified'"
                + " AND NOT EXISTS (SELECT 1 FROM " + target + " t WHERE t.edek_id = s.edek_id)");
        List<Object> insertArgs = new ArrayList<>();
        appendScope(insert, insertArgs, scope, "s.");
        try (PreparedStatement ps = prepare(insert.toString(), insertArgs)) {
            inserted = ps.executeUpdate();
        }
        StringBuilder mark = new StringBuilder("UPDATE " + staging + " SET migrate_status = 'promoted', "
                + "promoted_at = now() WHERE migrate_status = 'verified'");
        List<Object> markArgs = new ArrayList<>();
        appendScope(mark, markArgs, scope);
        try (PreparedStatement ps = prepare(mark.toString(), markArgs)) {
            ps.executeUpdate();
        }
        return new PromoteResult(false, blockers, 0, updated, inserted);
    }

    // ---- reset ----

    public int reset(Scope scope) throws SQLException {
        StringBuilder sql = new StringBuilder("DELETE FROM " + staging + " WHERE TRUE");
        List<Object> args = new ArrayList<>();
        appendScope(sql, args, scope);
        try (PreparedStatement ps = prepare(sql.toString(), args)) {
            return ps.executeUpdate();
        }
    }

    // ---- checks against B, for analyze ----

    /** A's rows whose edek_id is already in B's edek_records with another owner or fingerprint. */
    public List<UUID> idClashes(List<EdekRow> rows) throws SQLException {
        List<UUID> out = new ArrayList<>();
        if (rows.isEmpty()) {
            return out;
        }
        Map<UUID, EdekRow> byId = new HashMap<>();
        rows.forEach(r -> byId.put(r.edekId(), r));
        try (PreparedStatement ps = db.prepareStatement("SELECT edek_id, app_id, fingerprint FROM " + target
                + " WHERE edek_id = ANY(?)")) {
            ps.setArray(1, uuidArray(new ArrayList<>(byId.keySet())));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    EdekRow a = byId.get(rs.getObject(1, UUID.class));
                    if (!a.appId().equals(rs.getString(2)) || !java.util.Objects.equals(a.fingerprint(), rs.getString(3))) {
                        out.add(a.edekId());
                    }
                }
            }
        }
        return out;
    }

    /** A's current_dek_names that B's edek_records already gives to another edek_id. */
    public List<String> currentNameClashes(List<EdekRow> rows) throws SQLException {
        Map<String, UUID> names = new HashMap<>();
        for (EdekRow r : rows) {
            if (r.currentDekName() != null) {
                names.put(r.currentDekName(), r.edekId());
            }
        }
        List<String> out = new ArrayList<>();
        if (names.isEmpty()) {
            return out;
        }
        try (PreparedStatement ps = db.prepareStatement("SELECT current_dek_name, edek_id FROM " + target
                + " WHERE current_dek_name = ANY(?)")) {
            ps.setArray(1, db.createArrayOf("varchar", names.keySet().toArray()));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    if (!names.get(rs.getString(1)).equals(rs.getObject(2, UUID.class))) {
                        out.add(rs.getString(1));
                    }
                }
            }
        }
        return out;
    }

    /** Which of these app_ids have a row in B's app_registrations, and whether it's active. */
    public Map<String, Boolean> registeredApps(Set<String> appIds) throws SQLException {
        Map<String, Boolean> out = new HashMap<>();
        if (appIds.isEmpty()) {
            return out;
        }
        try (PreparedStatement ps = db.prepareStatement("SELECT app_id, active FROM " + targetAccess
                + ".app_registrations WHERE app_id = ANY(?)")) {
            ps.setArray(1, db.createArrayOf("varchar", appIds.toArray()));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.put(rs.getString(1), rs.getBoolean(2));
                }
            }
        }
        return out;
    }

    // ---- helpers ----

    private Array uuidArray(List<UUID> ids) throws SQLException {
        return db.createArrayOf("uuid", ids.toArray());
    }

    private static void appendScope(StringBuilder sql, List<Object> args, Scope scope) {
        appendScope(sql, args, scope, "");
    }

    private static void appendScope(StringBuilder sql, List<Object> args, Scope scope, String alias) {
        if (scope.kek() != null) {
            sql.append(" AND ").append(alias).append("source_kek_name = ?");
            args.add(scope.kek());
        }
        if (scope.app() != null) {
            sql.append(" AND ").append(alias).append("app_id = ?");
            args.add(scope.app());
        }
    }

    private PreparedStatement prepare(String sql, List<Object> args) throws SQLException {
        PreparedStatement ps = db.prepareStatement(sql);
        for (int i = 0; i < args.size(); i++) {
            ps.setObject(i + 1, args.get(i));
        }
        return ps;
    }
}
