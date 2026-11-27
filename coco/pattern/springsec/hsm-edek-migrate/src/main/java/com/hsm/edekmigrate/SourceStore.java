package com.hsm.edekmigrate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Reads A's {@code edek_records}. Never writes: the tool's source credential needs SELECT only. */
public final class SourceStore {

    private final Connection db;
    private final String table;
    private final String legacyKekName;

    public SourceStore(Connection db, String cryptoSchema, String legacyKekName) {
        this.db = db;
        this.table = cryptoSchema + ".edek_records";
        this.legacyKekName = legacyKekName;
    }

    /** The next page in edek_id order, after {@code after} (null for the first page). */
    public List<EdekRow> page(UUID after, int limit, Scope scope) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT " + EdekRow.COLUMNS + " FROM " + table + " WHERE TRUE");
        List<Object> args = new ArrayList<>();
        if (after != null) {
            sql.append(" AND edek_id > ?");
            args.add(after);
        }
        appendScope(sql, args, scope);
        sql.append(" ORDER BY edek_id LIMIT ?");
        args.add(limit);
        try (PreparedStatement ps = prepare(sql.toString(), args); ResultSet rs = ps.executeQuery()) {
            List<EdekRow> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(EdekRow.read(rs));
            }
            return rows;
        }
    }

    /** Row counts per resolved KEK name, KEK version, rotation_status and owner app_id. */
    public List<Count> counts(Scope scope) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT COALESCE(kek_name, ?) AS kek, kek_version, rotation_status, "
                + "app_id, COUNT(*) AS n FROM " + table + " WHERE TRUE");
        List<Object> args = new ArrayList<>();
        args.add(legacyKekName);
        appendScope(sql, args, scope);
        sql.append(" GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4");
        try (PreparedStatement ps = prepare(sql.toString(), args); ResultSet rs = ps.executeQuery()) {
            List<Count> out = new ArrayList<>();
            while (rs.next()) {
                out.add(new Count(rs.getString("kek"), rs.getString("kek_version"), rs.getString("rotation_status"),
                        rs.getString("app_id"), rs.getLong("n")));
            }
            return out;
        }
    }

    /** One row per distinct (resolved KEK name, KEK version), to probe unwrap access with. */
    public List<EdekRow> onePerKekVersion(Scope scope) throws SQLException {
        // The resolved name is computed once in the inner query: two "COALESCE(kek_name, ?)"
        // with separate parameters aren't the same expression to Postgres.
        StringBuilder sql = new StringBuilder("SELECT DISTINCT ON (resolved_kek, kek_version) " + EdekRow.COLUMNS
                + " FROM (SELECT *, COALESCE(kek_name, ?) AS resolved_kek FROM " + table + " WHERE TRUE");
        List<Object> args = new ArrayList<>();
        args.add(legacyKekName);
        appendScope(sql, args, scope);
        sql.append(") x ORDER BY resolved_kek, kek_version, edek_id");
        try (PreparedStatement ps = prepare(sql.toString(), args); ResultSet rs = ps.executeQuery()) {
            List<EdekRow> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(EdekRow.read(rs));
            }
            return rows;
        }
    }

    private void appendScope(StringBuilder sql, List<Object> args, Scope scope) {
        if (scope.kek() != null) {
            sql.append(" AND COALESCE(kek_name, ?) = ?");
            args.add(legacyKekName);
            args.add(scope.kek());
        }
        if (scope.app() != null) {
            sql.append(" AND app_id = ?");
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

    public record Count(String kekName, String kekVersion, String rotationStatus, String appId, long rows) {
    }
}
