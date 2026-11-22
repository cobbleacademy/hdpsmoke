package com.hsm.client.deidjob.source;

import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * A table or query, paged by key (DEIDENTIFICATION.md section 9), with the same keyset
 * SQL as the DB job: {@code WHERE key > ? ORDER BY key OFFSET 0 ROWS FETCH NEXT ? ROWS
 * ONLY}. A query is wrapped as a derived table and re-run for every page, so its key
 * must be unique and stable (see ClientProperties.Db.TableRef).
 *
 * <p>Only the columns the job asks for are selected ({@link #select}): a column the
 * profile doesn't list is never read. Column names are matched to the profile's
 * spelling ignoring case, because H2 and Oracle report unquoted names in upper case.
 *
 * <p>Values become strings: a date as {@code yyyy-MM-dd}, a timestamp as ISO
 * {@code yyyy-MM-ddTHH:mm:ss}, a decimal in plain notation, anything else by its
 * {@code toString}. A binary column fails: it can't be written to a CSV as text.
 */
public final class JdbcRecordSource implements RecordSource {

    private final JdbcTemplate jdbc;
    private final String from;
    private final String keyColumn;
    private final List<String> columns;
    private List<String> selected;
    private String selectSql;
    private String keyLabel;
    private Object lastKey;
    private boolean done;

    /**
     * @param from         a table name (optionally schema-qualified), or null when {@code query} is set
     * @param query        a full SELECT, or null
     * @param keyColumn    a unique, non-sensitive, sortable column
     * @param profileNames the profile's field names, so column names take the profile's spelling
     */
    public JdbcRecordSource(JdbcTemplate jdbc, String from, String query, String keyColumn,
                            Collection<String> profileNames) {
        if ((from == null || from.isBlank()) == (query == null || query.isBlank())) {
            throw new IllegalArgumentException("set exactly one of db.source.table or db.source.query");
        }
        if (keyColumn == null || keyColumn.isBlank()) {
            throw new IllegalArgumentException("db.key-column is required for a JDBC source");
        }
        this.jdbc = jdbc;
        // No "AS" before a derived table's alias: Oracle rejects it, the others accept both.
        this.from = query != null && !query.isBlank() ? "(" + query + ") src" : from;
        this.keyColumn = keyColumn;
        Map<String, String> byLower = new HashMap<>();
        for (String n : profileNames) {
            byLower.put(n.toLowerCase(Locale.ROOT), n);
        }
        List<String> names = jdbc.query("SELECT * FROM " + this.from + " WHERE 1 = 0", rs -> {
            ResultSetMetaData md = rs.getMetaData();
            List<String> out = new ArrayList<>();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                String label = md.getColumnLabel(i);
                out.add(byLower.getOrDefault(label.toLowerCase(Locale.ROOT), label));
            }
            return out;
        });
        this.columns = List.copyOf(names);
        if (columns.stream().noneMatch(c -> c.equalsIgnoreCase(keyColumn))) {
            throw new IllegalArgumentException("db.key-column '" + keyColumn + "' is not a column of the source");
        }
    }

    @Override
    public List<String> columns() {
        return columns;
    }

    /**
     * The columns to read. Call once, after validation and before the first page; the
     * key column is always read for paging but not returned unless asked for.
     */
    public void select(Collection<String> wanted) {
        Set<String> cols = new LinkedHashSet<>();
        for (String w : wanted) {
            columns.stream().filter(c -> c.equals(w)).findFirst().ifPresent(cols::add);
        }
        this.selected = List.copyOf(cols);
        this.keyLabel = columns.stream().filter(c -> c.equalsIgnoreCase(keyColumn)).findFirst().orElseThrow();
        StringBuilder sql = new StringBuilder("SELECT ").append(keyColumn).append(" AS deid_page_key");
        for (String c : selected) {
            sql.append(", ").append(c);
        }
        this.selectSql = sql.append(" FROM ").append(from).toString();
    }

    @Override
    public List<Map<String, String>> nextPage(int max) {
        if (selected == null) {
            throw new IllegalStateException("select(...) must be called before reading");
        }
        if (done) {
            return List.of();
        }
        StringBuilder sql = new StringBuilder(selectSql);
        List<Object> args = new ArrayList<>();
        if (lastKey != null) {
            sql.append(" WHERE ").append(keyColumn).append(" > ?");
            args.add(lastKey);
        }
        sql.append(" ORDER BY ").append(keyColumn).append(" OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY");
        args.add(max);
        List<Object> keys = new ArrayList<>();
        List<Map<String, String>> page = jdbc.query(sql.toString(), (rs, n) -> {
            keys.add(rs.getObject(1));
            Map<String, String> record = new HashMap<>(selected.size() * 2);
            for (int i = 0; i < selected.size(); i++) {
                record.put(selected.get(i), asString(rs.getObject(i + 2), selected.get(i)));
            }
            return record;
        }, args.toArray());
        if (page.size() < max) {
            done = true;
        }
        if (!keys.isEmpty()) {
            lastKey = keys.get(keys.size() - 1);
            if (lastKey == null) {
                throw new IllegalStateException("db.key-column '" + keyLabel + "' has a NULL value; it must be unique and not null");
            }
        }
        return page;
    }

    static String asString(Object v, String column) {
        try {
            return switch (v) {
                case null -> null;
                case String s -> s;
                case java.sql.Date d -> d.toLocalDate().toString();
                case Timestamp t -> t.toLocalDateTime().toString();
                case java.time.LocalDate d -> d.toString();
                case java.time.LocalDateTime t -> t.toString();
                case OffsetDateTime t -> t.toString();
                case BigDecimal b -> b.toPlainString();
                case Clob c -> c.getSubString(1, (int) c.length());
                case byte[] b -> throw new IllegalArgumentException("column '" + column
                        + "' is binary; it can't be written to a CSV (convert it in db.source.query)");
                default -> v.toString();
            };
        } catch (SQLException e) {
            throw new IllegalStateException("reading column '" + column + "' failed", e);
        }
    }

    @Override
    public void close() {
        // The DataSource belongs to the job.
    }
}
