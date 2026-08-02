package com.hsm.client.db;

import com.hsm.client.config.ClientProperties;
import com.hsm.client.crypto.DekManager;
import com.hsm.client.crypto.TransportWrapper;
import com.hsm.client.svc.SvcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.crypto.AEADBadTagException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * BULK DB job: reads plaintext from configured source columns, gets DEKs from SVC in
 * bulk (one DEK per column value, not per row -- preserves DEK-per-record isolation),
 * encrypts each value locally, writes ciphertext_token into the configured target
 * columns. decrypt() reverses it -- same config shape, source/target column meaning
 * flips (source = ciphertext_token column, target = plaintext column).
 *
 * <p>Keyset pagination (WHERE key_column &gt; ? ORDER BY key_column LIMIT ?), not
 * OFFSET -- avoids OFFSET's well-known large-table slowdown for big source tables.
 */
public class DbBulkJob {

    private static final Logger log = LoggerFactory.getLogger(DbBulkJob.class);

    private final ClientProperties.Db config;
    private final ClientProperties.Svc svcConfig;
    private final SvcClient svcClient;
    private final PrivateKey privateKey;
    private final JdbcTemplate sourceJdbc;
    private final JdbcTemplate targetJdbc;

    public DbBulkJob(ClientProperties.Db config, ClientProperties.Svc svcConfig, SvcClient svcClient) {
        this.config = config;
        this.svcConfig = svcConfig;
        this.svcClient = svcClient;
        this.privateKey = TransportWrapper.parsePrivateKeyPem(svcConfig.privateKeyPem());
        this.sourceJdbc = new JdbcTemplate(dataSource(config.source()));
        boolean sameTarget = config.target().jdbcUrl() == null || config.target().jdbcUrl().isBlank()
                || config.target().jdbcUrl().equals(config.source().jdbcUrl());
        this.targetJdbc = sameTarget ? sourceJdbc : new JdbcTemplate(dataSource(config.target()));
    }

    private static DriverManagerDataSource dataSource(ClientProperties.Db.TableRef ref) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setUrl(ref.jdbcUrl());
        if (ref.username() != null && !ref.username().isBlank()) {
            ds.setUsername(ref.username());
        }
        if (ref.password() != null) {
            ds.setPassword(ref.password());
        }
        return ds;
    }

    public void encrypt() {
        String sourceTable = qualify(config.source().schema(), config.source().table());
        String targetTable = qualify(config.target().schema(), config.target().table());
        List<String> sourceColumns = config.columns().stream().map(ClientProperties.Db.ColumnMapping::source).toList();
        List<String> passthroughColumns = passthroughColumns();
        List<String> selectColumns = concat(sourceColumns, passthroughColumns);
        List<String> targetColumns = concat(config.columns().stream().map(ClientProperties.Db.ColumnMapping::target).toList(), passthroughColumns);

        Object lastKey = null;
        int totalRows = 0;
        while (true) {
            List<Map<String, Object>> rows = fetchPage(sourceJdbc, sourceTable, config.keyColumn(), selectColumns, lastKey, config.rowBatchSize());
            if (rows.isEmpty()) {
                break;
            }

            for (List<Map<String, Object>> subBatch : subChunkByItemCap(rows, sourceColumns.size())) {
                List<SvcClient.IssueItem> issueItems = new ArrayList<>();
                for (Map<String, Object> row : subBatch) {
                    for (String col : sourceColumns) {
                        issueItems.add(new SvcClient.IssueItem(itemKey(row.get(config.keyColumn()), col), null));
                    }
                }
                List<SvcClient.IssueResult> issued = svcClient.issue(issueItems);
                Map<String, SvcClient.IssueResult> byKey = new LinkedHashMap<>();
                for (SvcClient.IssueResult r : issued) {
                    byKey.put(r.key(), r);
                }

                List<Object[]> targetRows = new ArrayList<>();
                for (Map<String, Object> row : subBatch) {
                    Object keyValue = row.get(config.keyColumn());
                    Object[] targetRow = new Object[1 + config.columns().size() + passthroughColumns.size()];
                    targetRow[0] = keyValue;
                    int i = 1;
                    for (ClientProperties.Db.ColumnMapping mapping : config.columns()) {
                        SvcClient.IssueResult result = byKey.get(itemKey(keyValue, mapping.source()));
                        if (result == null || !"success".equals(result.status())) {
                            throw new IllegalStateException("dek/issue failed for key=" + keyValue + " column=" + mapping.source()
                                    + ": " + (result == null ? "no result returned" : result.detail()));
                        }
                        byte[] dek = TransportWrapper.unwrap(Base64.getDecoder().decode(result.wrappedDekB64()), privateKey);
                        try {
                            Object plaintextValue = row.get(mapping.source());
                            String plaintext = plaintextValue == null ? null : plaintextValue.toString();
                            if (plaintext == null) {
                                targetRow[i++] = null;
                                continue;
                            }
                            DekManager.EncryptResult encrypted = DekManager.encrypt(
                                    plaintext.getBytes(StandardCharsets.UTF_8), dek, svcConfig.appId());
                            targetRow[i++] = DekManager.packToken(result.edekId(), encrypted.iv(), encrypted.tag(), encrypted.ciphertext());
                        } finally {
                            DekManager.zeroDek(dek);
                        }
                    }
                    for (String col : passthroughColumns) {
                        targetRow[i++] = row.get(col);
                    }
                    targetRows.add(targetRow);
                }
                insertRows(targetJdbc, targetTable, config.keyColumn(), targetColumns, targetRows);
            }

            totalRows += rows.size();
            lastKey = rows.get(rows.size() - 1).get(config.keyColumn());
            log.info("db_bulk_encrypt_progress rows_done={}", totalRows);
        }
        log.info("db_bulk_encrypt_complete total_rows={}", totalRows);
    }

    public void decrypt() {
        String sourceTable = qualify(config.source().schema(), config.source().table());
        String targetTable = qualify(config.target().schema(), config.target().table());
        List<String> sourceColumns = config.columns().stream().map(ClientProperties.Db.ColumnMapping::source).toList();
        List<String> passthroughColumns = passthroughColumns();
        List<String> selectColumns = concat(sourceColumns, passthroughColumns);
        List<String> targetColumns = concat(config.columns().stream().map(ClientProperties.Db.ColumnMapping::target).toList(), passthroughColumns);

        Object lastKey = null;
        int totalRows = 0;
        while (true) {
            List<Map<String, Object>> rows = fetchPage(sourceJdbc, sourceTable, config.keyColumn(), selectColumns, lastKey, config.rowBatchSize());
            if (rows.isEmpty()) {
                break;
            }

            for (List<Map<String, Object>> subBatch : subChunkByItemCap(rows, sourceColumns.size())) {
                Map<String, DekManager.UnpackedToken> unpackedByKey = new LinkedHashMap<>();
                List<SvcClient.UnwrapItem> unwrapItems = new ArrayList<>();
                for (Map<String, Object> row : subBatch) {
                    Object keyValue = row.get(config.keyColumn());
                    for (String col : sourceColumns) {
                        String token = Objects.toString(row.get(col), null);
                        if (token == null) {
                            continue;
                        }
                        DekManager.UnpackedToken unpacked = DekManager.unpackToken(token);
                        String k = itemKey(keyValue, col);
                        unpackedByKey.put(k, unpacked);
                        unwrapItems.add(new SvcClient.UnwrapItem(k, unpacked.edekId()));
                    }
                }
                if (unwrapItems.isEmpty()) {
                    continue;
                }
                List<SvcClient.UnwrapResult> unwrapped = svcClient.unwrap(unwrapItems);
                Map<String, SvcClient.UnwrapResult> byKey = new LinkedHashMap<>();
                for (SvcClient.UnwrapResult r : unwrapped) {
                    byKey.put(r.key(), r);
                }

                List<Object[]> targetRows = new ArrayList<>();
                for (Map<String, Object> row : subBatch) {
                    Object keyValue = row.get(config.keyColumn());
                    Object[] targetRow = new Object[1 + config.columns().size() + passthroughColumns.size()];
                    targetRow[0] = keyValue;
                    int i = 1;
                    for (ClientProperties.Db.ColumnMapping mapping : config.columns()) {
                        String k = itemKey(keyValue, mapping.source());
                        DekManager.UnpackedToken unpacked = unpackedByKey.get(k);
                        SvcClient.UnwrapResult result = byKey.get(k);
                        if (unpacked == null) {
                            targetRow[i++] = null;
                            continue;
                        }
                        if (result == null || !"success".equals(result.status())) {
                            throw new IllegalStateException("dek/unwrap failed for key=" + keyValue + " column=" + mapping.source()
                                    + ": " + (result == null ? "no result returned" : result.detail()));
                        }
                        byte[] dek = TransportWrapper.unwrap(Base64.getDecoder().decode(result.wrappedDekB64()), privateKey);
                        try {
                            byte[] plaintext = DekManager.decrypt(unpacked.ciphertext(), unpacked.tag(), unpacked.iv(), dek, svcConfig.appId());
                            String decrypted = new String(plaintext, StandardCharsets.UTF_8);
                            targetRow[i++] = convertForTarget(decrypted, mapping.targetType());
                        } catch (AEADBadTagException e) {
                            throw new IllegalStateException("AEAD tag verification failed for key=" + keyValue + " column=" + mapping.source(), e);
                        } catch (IllegalArgumentException e) {
                            // Date.valueOf/Timestamp.valueOf/NumberFormatException (a NumberFormatException
                            // IS an IllegalArgumentException) all land here -- one catch covers every
                            // TargetType's parse failure.
                            throw new IllegalStateException("Decrypted value is not a valid " + mapping.targetType()
                                    + " for key=" + keyValue + " column=" + mapping.source(), e);
                        } finally {
                            DekManager.zeroDek(dek);
                        }
                    }
                    for (String col : passthroughColumns) {
                        targetRow[i++] = row.get(col);
                    }
                    targetRows.add(targetRow);
                }
                insertRows(targetJdbc, targetTable, config.keyColumn(), targetColumns, targetRows);
            }

            totalRows += rows.size();
            lastKey = rows.get(rows.size() - 1).get(config.keyColumn());
            log.info("db_bulk_decrypt_progress rows_done={}", totalRows);
        }
        log.info("db_bulk_decrypt_complete total_rows={}", totalRows);
    }

    /** Sub-chunks a row-batch so rows.size() * columnsPerRow never exceeds svc.dek-batch-max-items in one /dek/issue or /dek/unwrap call. */
    private List<List<Map<String, Object>>> subChunkByItemCap(List<Map<String, Object>> rows, int columnsPerRow) {
        int maxRowsPerCall = Math.max(1, svcConfig.dekBatchMaxItems() / Math.max(1, columnsPerRow));
        List<List<Map<String, Object>>> chunks = new ArrayList<>();
        for (int i = 0; i < rows.size(); i += maxRowsPerCall) {
            chunks.add(rows.subList(i, Math.min(i + maxRowsPerCall, rows.size())));
        }
        return chunks;
    }

    /** Empty, never null -- config.passthroughColumns() binds to null when omitted from YAML entirely. */
    private List<String> passthroughColumns() {
        return config.passthroughColumns() == null ? List.of() : config.passthroughColumns();
    }

    /**
     * DekManager.decrypt() always hands back the original plaintext as raw UTF-8 bytes
     * -- there's no way to recover the source column's real SQL type from the
     * ciphertext alone, so the caller (the DECRYPT job's own config) has to say what
     * the target column actually is via ColumnMapping.targetType. Parsed value is
     * handed to JDBC via plain setObject (see insertRows) -- a real java.sql.Date/
     * Timestamp/BigDecimal/Long object round-trips into its matching column type
     * correctly, where the raw decrypted String would not (see the config-examples'
     * passthrough-columns note for why an untyped String into a non-VARCHAR column
     * fails).
     *
     * <p>Date.valueOf/Timestamp.valueOf deliberately used directly, not via
     * LocalDate/LocalDateTime.parse -- they accept exactly the format
     * java.sql.Date.toString()/Timestamp.toString() produce (what ENCRYPT already
     * stringified the original column value with), so this is a true round trip, not
     * a reformat that could drift from what ENCRYPT actually saw.
     */
    private static Object convertForTarget(String value, ClientProperties.Db.ColumnMapping.TargetType targetType) {
        ClientProperties.Db.ColumnMapping.TargetType type =
                targetType == null ? ClientProperties.Db.ColumnMapping.TargetType.STRING : targetType;
        return switch (type) {
            case STRING -> value;
            case DATE -> Date.valueOf(value);
            case TIMESTAMP -> Timestamp.valueOf(value);
            case NUMERIC -> new BigDecimal(value);
            case INTEGER -> Long.parseLong(value);
        };
    }

    private static List<String> concat(List<String> a, List<String> b) {
        List<String> combined = new ArrayList<>(a);
        combined.addAll(b);
        return combined;
    }

    private static String itemKey(Object keyValue, String column) {
        return keyValue + "#" + column;
    }

    private static String qualify(String schema, String table) {
        return (schema == null || schema.isBlank()) ? table : schema + "." + table;
    }

    private List<Map<String, Object>> fetchPage(JdbcTemplate jdbc, String table, String keyColumn, List<String> columns, Object afterKey, int limit) {
        String columnList = String.join(", ", columns);
        String sql = "SELECT " + keyColumn + ", " + columnList + " FROM " + table
                + (afterKey == null ? "" : " WHERE " + keyColumn + " > ?")
                + " ORDER BY " + keyColumn + " LIMIT ?";
        Object[] args = afterKey == null ? new Object[]{limit} : new Object[]{afterKey, limit};
        return jdbc.queryForList(sql, args);
    }

    private void insertRows(JdbcTemplate jdbc, String table, String keyColumn, List<String> targetColumns, List<Object[]> rows) {
        if (rows.isEmpty()) {
            return;
        }
        String columnList = keyColumn + ", " + String.join(", ", targetColumns);
        String placeholders = String.join(", ", targetColumns.stream().map(c -> "?").toList());
        String sql = "INSERT INTO " + table + " (" + columnList + ") VALUES (?, " + placeholders + ")";
        jdbc.batchUpdate(sql, rows);
    }
}
