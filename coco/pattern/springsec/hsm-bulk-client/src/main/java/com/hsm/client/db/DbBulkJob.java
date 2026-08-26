package com.hsm.client.db;

import com.hsm.client.config.ClientProperties;
import com.hsm.client.crypto.DekManager;
import com.hsm.client.crypto.TransportWrapper;
import com.hsm.client.svc.SvcClient;
import com.hsm.client.svc.SvcConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

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
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * BULK DB job: reads plaintext from configured source columns, gets DEKs from SVC in
 * bulk (one DEK per column value, not per row -- preserves DEK-per-record isolation),
 * encrypts each value locally, writes the ciphertext token into the configured target
 * columns. decrypt() reverses it -- same config shape, source/target column meaning
 * flips (source = ciphertext column, target = plaintext column).
 *
 * <p>Keyset pagination (WHERE key_column &gt; ? ORDER BY key_column ... FETCH NEXT ?
 * ROWS ONLY), not OFFSET-as-position -- avoids OFFSET's well-known large-table
 * slowdown for big source tables. The row-count cap itself uses the ANSI SQL:2008
 * OFFSET/FETCH clause (OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY) rather than Postgres's
 * LIMIT shorthand -- this form is portable across every dialect this module
 * supports (Postgres, SQL Server 2012+, Oracle 12c+) with zero branching, unlike
 * the checkpoint SQL in CheckpointStore, which has no such portable common form.
 *
 * <p>config.parallelism() &gt; 1 partitions the key range into that many pieces up
 * front (a bounded, one-time set of OFFSET lookups to find partition boundaries --
 * not the per-page anti-pattern the keyset pagination above avoids) and runs one
 * independent worker per partition concurrently, each doing the exact same
 * fetch-issue-encrypt-insert pipeline as the sequential (parallelism=1) path,
 * scoped to its own key range.
 */
public class DbBulkJob {

    private static final Logger log = LoggerFactory.getLogger(DbBulkJob.class);

    private final ClientProperties.Db config;
    private final SvcConfig svcConfig;
    private final SvcClient svcClient;
    private final PrivateKey privateKey;
    private final JdbcTemplate sourceJdbc;
    private final JdbcTemplate targetJdbc;
    // Both null when config.checkpoint() is absent or checkpoint.enabled=false -- the
    // pre-existing, zero-overhead behavior. checkpointEnabled() is the single source
    // of truth callers check, not a null-check on config.checkpoint() directly.
    private final CheckpointStore checkpointStore;
    private final TransactionTemplate targetTxTemplate;
    // Shared across every partition worker for the lifetime of one decrypt() call --
    // without this, a column configured with dek-name (a config-level guarantee that
    // every row's edek_id for that column is the SAME shared DEK) gets re-fetched via
    // /dek/unwrap AND re-unwrapped (RSA-OAEP-256, the dominant cost per the original
    // bulk-vs-batch benchmark) on every single row sub-batch, even though the exact
    // same edek_id was already unwrapped moments earlier -- the existing per-sub-batch
    // dedup only helps within one sub-batch, never across them. Deliberately scoped to
    // ONLY dek-name columns, not every edek_id decrypt ever sees: an unnamed column's
    // DEK is genuinely one-off per row by design, so caching it here would just grow
    // memory for zero reuse benefit -- see isNamed(ColumnMapping) for the same
    // named/unnamed distinction the encrypt path already makes. Plain, unbounded map
    // is fine (not LRU) -- its size is bounded by the config's own distinct dek-name
    // count, typically a handful, the same assumption issueNamedColumnDeks() makes.
    private final Map<UUID, byte[]> namedColumnDekCache = new ConcurrentHashMap<>();

    public DbBulkJob(ClientProperties.Db config, SvcConfig svcConfig, SvcClient svcClient) {
        this.config = config;
        this.svcConfig = svcConfig;
        this.svcClient = svcClient;
        this.privateKey = TransportWrapper.parsePrivateKeyPem(svcConfig.privateKeyPem());
        // Pool must cover every concurrent worker (parallelism) plus headroom for the
        // one-time partition-boundary lookups issued on sourceJdbc before workers start.
        int poolSize = Math.max(1, config.parallelism()) + 2;
        this.sourceJdbc = new JdbcTemplate(dataSource(config.source(), poolSize));
        boolean sameTarget = config.target().jdbcUrl() == null || config.target().jdbcUrl().isBlank()
                || config.target().jdbcUrl().equals(config.source().jdbcUrl());
        this.targetJdbc = sameTarget ? sourceJdbc : new JdbcTemplate(dataSource(config.target(), poolSize));
        if (checkpointEnabled(config)) {
            // Checkpoint state always lives in the TARGET database (see
            // resolveInitialLastKey/insertRowsWithCheckpoint -- every checkpoint
            // operation runs against targetJdbc), so only the target's dialect
            // matters here, never source's.
            DbDialect targetDialect = DbDialect.resolve(config.target().jdbcUrl(), config.target().dialect());
            this.checkpointStore = new CheckpointStore(
                    qualify(config.checkpoint().schema(), config.checkpoint().tableName()), targetDialect);
            this.targetTxTemplate = new TransactionTemplate(new DataSourceTransactionManager(targetJdbc.getDataSource()));
        } else {
            this.checkpointStore = null;
            this.targetTxTemplate = null;
        }
    }

    private static boolean checkpointEnabled(ClientProperties.Db config) {
        return config.checkpoint() != null && config.checkpoint().enabled();
    }

    private boolean checkpointEnabled() {
        return checkpointStore != null;
    }

    private static HikariDataSource dataSource(ClientProperties.Db.TableRef ref, int poolSize) {
        HikariConfig hc = new HikariConfig();
        DbDialect dialect = DbDialect.resolve(ref.jdbcUrl(), ref.dialect());
        String url = dialect == DbDialect.POSTGRESQL ? withReWriteBatchedInserts(ref.jdbcUrl()) : ref.jdbcUrl();
        hc.setJdbcUrl(url);
        if (ref.username() != null && !ref.username().isBlank()) {
            hc.setUsername(ref.username());
        }
        if (ref.password() != null) {
            hc.setPassword(ref.password());
        }
        hc.setMaximumPoolSize(poolSize);
        // Never logs the password -- url/username only, and url itself never carries
        // credentials in this module's config shape (see TableRef: username/password
        // are always separate fields, never embedded in jdbcUrl).
        log.info("db_bulk_pool_configured url={} username={} pool_size={}", url, ref.username(), poolSize);
        return new HikariDataSource(hc);
    }

    /**
     * PgJDBC's batch protocol is still N separate bind/execute messages per flush
     * without this -- reWriteBatchedInserts=true makes jdbc.batchUpdate's underlying
     * PreparedStatement.executeBatch() actually rewrite into true multi-row
     * INSERT ... VALUES (...),(...),(...) statements. Postgres-specific (a no-op
     * append on another driver would just be an unrecognized parameter) -- the call
     * site above only invokes this when the resolved DbDialect is POSTGRESQL, so
     * this method's own startsWith check below is a second, redundant guard, not the
     * only one. SQL Server/Oracle each have their own real equivalents (SQL Server's
     * useBulkCopyForBatchInsert, Oracle's array-DML batching) -- deliberately not
     * added here, out of scope for "support the three dialects correctly" versus a
     * per-vendor batch-insert performance tuning pass.
     */
    private static String withReWriteBatchedInserts(String jdbcUrl) {
        if (jdbcUrl == null || !jdbcUrl.startsWith("jdbc:postgresql:") || jdbcUrl.contains("reWriteBatchedInserts")) {
            return jdbcUrl;
        }
        return jdbcUrl + (jdbcUrl.contains("?") ? "&" : "?") + "reWriteBatchedInserts=true";
    }

    public void encrypt() {
        String sourceTable = resolveSourceFrom(config.source());
        String targetTable = qualify(config.target().schema(), config.target().table());

        // One DEK per distinct dek-name for the WHOLE run, resolved/unwrapped once up
        // front -- not once per row, not even once per sub-batch, and shared read-only
        // across every parallel worker. SVC's own /dek/issue already reuses the
        // current DEK for a name idempotently, so calling it more often would still
        // be correct, just redundant.
        Map<String, NamedDek> namedDeks = issueNamedColumnDeks();
        long startMs = System.currentTimeMillis();
        AtomicLong rowsDone = new AtomicLong();
        try {
            runPartitioned(sourceTable, "encrypt", rowsDone,
                    (rangeStart, rangeEnd, jobId) -> encryptRange(sourceTable, targetTable, namedDeks, rangeStart, rangeEnd, jobId, rowsDone));
            logCompletion("encrypt", rowsDone.get(), startMs);
        } finally {
            namedDeks.values().forEach(nd -> DekManager.zeroDek(nd.dek()));
        }
    }

    public void decrypt() {
        String sourceTable = resolveSourceFrom(config.source());
        String targetTable = qualify(config.target().schema(), config.target().table());

        long startMs = System.currentTimeMillis();
        AtomicLong rowsDone = new AtomicLong();
        try {
            runPartitioned(sourceTable, "decrypt", rowsDone,
                    (rangeStart, rangeEnd, jobId) -> decryptRange(sourceTable, targetTable, rangeStart, rangeEnd, jobId, rowsDone));
            logCompletion("decrypt", rowsDone.get(), startMs);
        } finally {
            namedColumnDekCache.values().forEach(DekManager::zeroDek);
        }
    }

    private void logCompletion(String direction, long totalRows, long startMs) {
        long elapsedMs = System.currentTimeMillis() - startMs;
        double rowsPerSec = elapsedMs > 0 ? totalRows * 1000.0 / elapsedMs : 0;
        log.info("db_bulk_{}_complete total_rows={} elapsed_ms={} rows_per_sec={}",
                direction, totalRows, elapsedMs, String.format("%.1f", rowsPerSec));
    }

    @FunctionalInterface
    private interface RangeWorker {
        void run(Object rangeStart, Object rangeEnd, String jobId);
    }

    /**
     * parallelism &lt;= 1 (default): runs the one worker inline, unbounded range
     * (null, null) -- identical code path and behavior to before parallelism existed.
     * parallelism &gt; 1: partitions the key range and runs one worker per partition
     * concurrently, propagating the first failure and waiting for all to finish.
     */
    private void runPartitioned(String sourceTable, String direction, AtomicLong rowsDone, RangeWorker worker) {
        int parallelism = Math.max(1, config.parallelism());
        if (parallelism <= 1) {
            String jobId = checkpointEnabled() ? config.checkpoint().jobId() : null;
            worker.run(null, null, jobId);
            return;
        }

        List<KeyRange> ranges = computePartitionRanges(sourceTable, parallelism);
        log.info("db_bulk_{}_parallel_start partitions={}", direction, ranges.size());
        ExecutorService pool = Executors.newFixedThreadPool(ranges.size());
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int p = 0; p < ranges.size(); p++) {
                KeyRange range = ranges.get(p);
                String workerJobId = checkpointEnabled() ? config.checkpoint().jobId() + "-p" + p : null;
                futures.add(pool.submit(() -> worker.run(range.startExclusive(), range.endInclusive(), workerJobId)));
            }
            RuntimeException firstFailure = null;
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    RuntimeException wrapped = new IllegalStateException("parallel worker failed: " + e.getCause(), e.getCause());
                    if (firstFailure == null) {
                        firstFailure = wrapped;
                    } else {
                        firstFailure.addSuppressed(wrapped);
                    }
                }
            }
            if (firstFailure != null) {
                throw firstFailure;
            }
        } finally {
            pool.shutdown();
        }
    }

    private record KeyRange(Object startExclusive, Object endInclusive) {
    }

    /**
     * Finds parallelism-1 boundary keys via ORDER BY key_column OFFSET x LIMIT 1 --
     * a bounded, one-time set of lookups (at most parallelism-1 queries total),
     * fundamentally different from using OFFSET as the main pagination mechanism
     * (which the rest of this class deliberately avoids for large-table performance).
     * Falls back to fewer partitions than requested if the table has fewer rows.
     */
    private List<KeyRange> computePartitionRanges(String sourceTable, int parallelism) {
        Long totalRows = sourceJdbc.queryForObject("SELECT COUNT(*) FROM " + sourceTable, Long.class);
        if (totalRows == null || totalRows == 0) {
            return List.of(new KeyRange(null, null));
        }
        int actualPartitions = (int) Math.max(1, Math.min(parallelism, totalRows));
        List<Object> boundaries = new ArrayList<>();
        for (int i = 1; i < actualPartitions; i++) {
            long offset = totalRows * i / actualPartitions;
            // ANSI SQL:2008 OFFSET/FETCH, not Postgres's LIMIT shorthand -- portable
            // across every dialect this module supports, see DbBulkJob's class
            // javadoc. offset itself is dynamic (the boundary lookup position);
            // fetching exactly 1 row is always a literal, never bound.
            List<Object> row = sourceJdbc.queryForList(
                    "SELECT " + config.keyColumn() + " FROM " + sourceTable
                            + " ORDER BY " + config.keyColumn() + " OFFSET ? ROWS FETCH NEXT 1 ROWS ONLY",
                    Object.class, offset);
            if (!row.isEmpty()) {
                boundaries.add(row.get(0));
            }
        }
        List<KeyRange> ranges = new ArrayList<>();
        Object prev = null;
        for (Object boundary : boundaries) {
            ranges.add(new KeyRange(prev, boundary));
            prev = boundary;
        }
        ranges.add(new KeyRange(prev, null));
        return ranges;
    }

    /**
     * Reads any prior checkpoint (resume=true) or clears it for a fresh start
     * (resume=false), returning the key to resume pagination after. fallbackStart is
     * null for the sequential path (start from the first row) or a partition's own
     * lower bound for the parallel path -- either way, that's what's used when
     * checkpointing is disabled entirely or no prior checkpoint exists yet.
     */
    private Object resolveInitialLastKey(String jobId, Object fallbackStart) {
        if (!checkpointEnabled()) {
            return fallbackStart;
        }
        ClientProperties.Db.Checkpoint cp = config.checkpoint();
        checkpointStore.ensureTable(targetJdbc);
        if (!cp.resume()) {
            checkpointStore.clear(targetJdbc, jobId);
            return fallbackStart;
        }
        Optional<String> saved = checkpointStore.loadLastKey(targetJdbc, jobId);
        if (saved.isEmpty()) {
            return fallbackStart;
        }
        Object resumedKey = convertForTarget(saved.get(), config.keyColumnType());
        log.info("db_bulk_resume job_id={} last_key={}", jobId, resumedKey);
        return resumedKey;
    }

    /**
     * When checkpointing is enabled, the data batch and its checkpoint update commit
     * in one transaction -- either both land durably or neither does, so a resumed
     * run can never skip rows that were never written, nor re-process rows that were.
     * When disabled, this is exactly the original, uncoupled insertRows call.
     */
    private void insertRowsWithCheckpoint(String jobId, String targetTable, String keyColumn, List<String> targetColumns,
                                           List<Object[]> targetRows, Object batchLastKey, long rowsDone) {
        if (!checkpointEnabled()) {
            insertRows(targetJdbc, targetTable, keyColumn, targetColumns, targetRows);
            return;
        }
        targetTxTemplate.executeWithoutResult(status -> {
            insertRows(targetJdbc, targetTable, keyColumn, targetColumns, targetRows);
            checkpointStore.save(targetJdbc, jobId, batchLastKey, rowsDone);
        });
    }

    private record NamedDek(UUID edekId, byte[] dek) {
    }

    private static boolean isNamed(ClientProperties.Db.ColumnMapping mapping) {
        return mapping.dekName() != null && !mapping.dekName().isBlank();
    }

    /** One /dek/issue call for every distinct dek-name across config.columns() -- deduped so two columns sharing one name don't send a duplicate-key request SVC would reject. */
    private Map<String, NamedDek> issueNamedColumnDeks() {
        List<String> distinctNames = config.columns().stream()
                .map(ClientProperties.Db.ColumnMapping::dekName)
                .filter(n -> n != null && !n.isBlank())
                .distinct()
                .toList();
        if (distinctNames.isEmpty()) {
            return Map.of();
        }
        List<SvcClient.IssueItem> issueItems = distinctNames.stream()
                .map(name -> new SvcClient.IssueItem(name, null, name))
                .toList();
        List<SvcClient.IssueResult> issued = svcClient.issue(issueItems);
        Map<String, NamedDek> result = new LinkedHashMap<>();
        for (SvcClient.IssueResult r : issued) {
            if (!"success".equals(r.status())) {
                throw new IllegalStateException("dek/issue failed for dek-name=" + r.key() + ": " + r.detail());
            }
            byte[] dek = TransportWrapper.unwrap(Base64.getDecoder().decode(r.wrappedDekB64()), privateKey);
            result.put(r.key(), new NamedDek(r.edekId(), dek));
            log.info("db_bulk_named_dek_resolved dek_name={} reused={}", r.key(), r.reused());
        }
        return result;
    }

    /** One partition's (or, when parallelism=1, the whole table's) fetch-issue-encrypt-insert pipeline. */
    private void encryptRange(String sourceTable, String targetTable, Map<String, NamedDek> namedDeks,
                               Object rangeStart, Object rangeEnd, String jobId, AtomicLong rowsDoneTotal) {
        List<String> sourceColumns = config.columns().stream().map(ClientProperties.Db.ColumnMapping::source).toList();
        List<String> passthroughColumns = passthroughColumns();
        List<String> selectColumns = concat(sourceColumns, passthroughColumns);
        List<String> targetColumns = concat(config.columns().stream().map(ClientProperties.Db.ColumnMapping::target).toList(), passthroughColumns);
        List<String> unnamedSourceColumns = config.columns().stream()
                .filter(m -> !isNamed(m))
                .map(ClientProperties.Db.ColumnMapping::source)
                .toList();

        Object lastKey = resolveInitialLastKey(jobId, rangeStart);
        long partitionRowsDone = 0;
        while (true) {
            List<Map<String, Object>> rows = fetchPage(sourceJdbc, sourceTable, config.keyColumn(), selectColumns, lastKey, rangeEnd, config.rowBatchSize());
            if (rows.isEmpty()) {
                break;
            }

            for (List<Map<String, Object>> subBatch : subChunkByItemCap(rows, unnamedSourceColumns.size())) {
                List<SvcClient.IssueItem> issueItems = new ArrayList<>();
                for (Map<String, Object> row : subBatch) {
                    for (String col : unnamedSourceColumns) {
                        issueItems.add(new SvcClient.IssueItem(itemKey(row.get(config.keyColumn()), col), null, null));
                    }
                }
                List<SvcClient.IssueResult> issued = issueItems.isEmpty() ? List.of() : svcClient.issue(issueItems);
                Map<String, SvcClient.IssueResult> byKey = new LinkedHashMap<>();
                for (SvcClient.IssueResult r : issued) {
                    byKey.put(r.key(), r);
                }

                boolean includeKey = !config.skipKeyColumnOnTarget();
                List<Object[]> targetRows = new ArrayList<>();
                for (Map<String, Object> row : subBatch) {
                    Object keyValue = row.get(config.keyColumn());
                    Object[] targetRow = new Object[(includeKey ? 1 : 0) + config.columns().size() + passthroughColumns.size()];
                    int i = 0;
                    if (includeKey) {
                        targetRow[i++] = keyValue;
                    }
                    for (ClientProperties.Db.ColumnMapping mapping : config.columns()) {
                        Object plaintextValue = row.get(mapping.source());
                        String plaintext = plaintextValue == null ? null : plaintextValue.toString();
                        if (plaintext == null) {
                            targetRow[i++] = null;
                            continue;
                        }
                        if (isNamed(mapping)) {
                            NamedDek namedDek = namedDeks.get(mapping.dekName());
                            DekManager.EncryptResult encrypted = DekManager.encrypt(
                                    plaintext.getBytes(StandardCharsets.UTF_8), namedDek.dek(), svcConfig.appId());
                            targetRow[i++] = DekManager.packToken(namedDek.edekId(), encrypted.iv(), encrypted.tag(), encrypted.ciphertext());
                        } else {
                            SvcClient.IssueResult result = byKey.get(itemKey(keyValue, mapping.source()));
                            if (result == null || !"success".equals(result.status())) {
                                throw new IllegalStateException("dek/issue failed for key=" + keyValue + " column=" + mapping.source()
                                        + ": " + (result == null ? "no result returned" : result.detail()));
                            }
                            byte[] dek = TransportWrapper.unwrap(Base64.getDecoder().decode(result.wrappedDekB64()), privateKey);
                            try {
                                DekManager.EncryptResult encrypted = DekManager.encrypt(
                                        plaintext.getBytes(StandardCharsets.UTF_8), dek, svcConfig.appId());
                                targetRow[i++] = DekManager.packToken(result.edekId(), encrypted.iv(), encrypted.tag(), encrypted.ciphertext());
                            } finally {
                                DekManager.zeroDek(dek);
                            }
                        }
                    }
                    for (String col : passthroughColumns) {
                        targetRow[i++] = row.get(col);
                    }
                    targetRows.add(targetRow);
                }
                partitionRowsDone += subBatch.size();
                rowsDoneTotal.addAndGet(subBatch.size());
                Object subBatchLastKey = subBatch.get(subBatch.size() - 1).get(config.keyColumn());
                insertRowsWithCheckpoint(jobId, targetTable, targetKeyColumnOrNull(), targetColumns, targetRows, subBatchLastKey, partitionRowsDone);
            }

            lastKey = rows.get(rows.size() - 1).get(config.keyColumn());
            log.info("db_bulk_encrypt_progress job_id={} rows_done={}", jobId, rowsDoneTotal.get());
        }
    }

    /** One partition's (or, when parallelism=1, the whole table's) fetch-unwrap-decrypt-insert pipeline. */
    private void decryptRange(String sourceTable, String targetTable, Object rangeStart, Object rangeEnd,
                               String jobId, AtomicLong rowsDoneTotal) {
        List<String> sourceColumns = config.columns().stream().map(ClientProperties.Db.ColumnMapping::source).toList();
        List<String> passthroughColumns = passthroughColumns();
        List<String> selectColumns = concat(sourceColumns, passthroughColumns);
        List<String> targetColumns = concat(config.columns().stream().map(ClientProperties.Db.ColumnMapping::target).toList(), passthroughColumns);
        // Same fast-path reasoning as encrypt's unnamedSourceColumns (see encryptRange):
        // once namedColumnDekCache is warm, a dek-name'd column needs zero further
        // /dek/unwrap calls for the rest of the job, so it shouldn't count toward the
        // item cap that exists purely to bound the SIZE of an actual HTTP call. Using
        // the full sourceColumns.size() here regardless of naming was an oversight --
        // it kept artificially sub-chunking every page (smaller inserts, smaller
        // checkpoint commits, more round trips) even for a fully-named run where, after
        // the first page, not a single sub-batch needs a real network call at all.
        long unnamedColumnCount = config.columns().stream().filter(m -> !isNamed(m)).count();

        Object lastKey = resolveInitialLastKey(jobId, rangeStart);
        long partitionRowsDone = 0;
        while (true) {
            List<Map<String, Object>> rows = fetchPage(sourceJdbc, sourceTable, config.keyColumn(), selectColumns, lastKey, rangeEnd, config.rowBatchSize());
            if (rows.isEmpty()) {
                break;
            }

            for (List<Map<String, Object>> subBatch : subChunkByItemCap(rows, (int) unnamedColumnCount)) {
                Map<String, DekManager.UnpackedToken> unpackedByKey = new LinkedHashMap<>();
                // Tracks which edek_ids showed up under a dek-name-configured column --
                // only those are eligible for the persistent namedColumnDekCache. An
                // unnamed column's DEK is genuinely one-off per row by config-level
                // guarantee, so it never touches the cache, same as before this change.
                Set<UUID> namedColumnEdekIds = new HashSet<>();
                for (Map<String, Object> row : subBatch) {
                    Object keyValue = row.get(config.keyColumn());
                    for (ClientProperties.Db.ColumnMapping mapping : config.columns()) {
                        String token = Objects.toString(row.get(mapping.source()), null);
                        if (token == null) {
                            continue;
                        }
                        DekManager.UnpackedToken unpacked = DekManager.unpackToken(token);
                        unpackedByKey.put(itemKey(keyValue, mapping.source()), unpacked);
                        if (isNamed(mapping)) {
                            namedColumnEdekIds.add(unpacked.edekId());
                        }
                    }
                }
                if (unpackedByKey.isEmpty()) {
                    continue;
                }

                // Dedup by edek_id, not by (row, column) -- many pairs can share one id
                // under a named/reused DEK. One /dek/unwrap call AND one local RSA-OAEP
                // unwrap per distinct id, not one per pair -- RSA unwrap was the
                // dominant cost in the original bulk-vs-batch benchmark, so this is the
                // more important half of the dedup, not just fewer HTTP items.
                List<UUID> distinctEdekIds = unpackedByKey.values().stream()
                        .map(DekManager.UnpackedToken::edekId)
                        .distinct()
                        .toList();

                Map<UUID, byte[]> dekByEdekId = new LinkedHashMap<>();
                for (UUID id : distinctEdekIds) {
                    if (namedColumnEdekIds.contains(id)) {
                        byte[] cached = namedColumnDekCache.get(id);
                        if (cached != null) {
                            dekByEdekId.put(id, cached);
                        }
                    }
                }
                List<UUID> toFetch = distinctEdekIds.stream().filter(id -> !dekByEdekId.containsKey(id)).toList();
                List<SvcClient.UnwrapItem> unwrapItems = toFetch.stream()
                        .map(id -> new SvcClient.UnwrapItem(id.toString(), id))
                        .toList();
                List<SvcClient.UnwrapResult> unwrapped = unwrapItems.isEmpty() ? List.of() : svcClient.unwrap(unwrapItems);
                Map<UUID, SvcClient.UnwrapResult> resultByEdekId = new LinkedHashMap<>();
                for (SvcClient.UnwrapResult r : unwrapped) {
                    resultByEdekId.put(UUID.fromString(r.key()), r);
                }

                try {
                    for (Map.Entry<UUID, SvcClient.UnwrapResult> e : resultByEdekId.entrySet()) {
                        if ("success".equals(e.getValue().status())) {
                            byte[] dek = TransportWrapper.unwrap(
                                    Base64.getDecoder().decode(e.getValue().wrappedDekB64()), privateKey);
                            dekByEdekId.put(e.getKey(), dek);
                            if (namedColumnEdekIds.contains(e.getKey())) {
                                namedColumnDekCache.put(e.getKey(), dek);
                            }
                        }
                    }

                    boolean includeKey = !config.skipKeyColumnOnTarget();
                    List<Object[]> targetRows = new ArrayList<>();
                    for (Map<String, Object> row : subBatch) {
                        Object keyValue = row.get(config.keyColumn());
                        Object[] targetRow = new Object[(includeKey ? 1 : 0) + config.columns().size() + passthroughColumns.size()];
                        int i = 0;
                        if (includeKey) {
                            targetRow[i++] = keyValue;
                        }
                        for (ClientProperties.Db.ColumnMapping mapping : config.columns()) {
                            String k = itemKey(keyValue, mapping.source());
                            DekManager.UnpackedToken unpacked = unpackedByKey.get(k);
                            if (unpacked == null) {
                                targetRow[i++] = null;
                                continue;
                            }
                            byte[] dek = dekByEdekId.get(unpacked.edekId());
                            if (dek == null) {
                                SvcClient.UnwrapResult result = resultByEdekId.get(unpacked.edekId());
                                throw new IllegalStateException("dek/unwrap failed for key=" + keyValue + " column=" + mapping.source()
                                        + ": " + (result == null ? "no result returned" : result.detail()));
                            }
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
                            }
                        }
                        for (String col : passthroughColumns) {
                            targetRow[i++] = row.get(col);
                        }
                        targetRows.add(targetRow);
                    }
                    partitionRowsDone += subBatch.size();
                    rowsDoneTotal.addAndGet(subBatch.size());
                    Object subBatchLastKey = subBatch.get(subBatch.size() - 1).get(config.keyColumn());
                    insertRowsWithCheckpoint(jobId, targetTable, targetKeyColumnOrNull(), targetColumns, targetRows, subBatchLastKey, partitionRowsDone);
                } finally {
                    // Only zero DEKs NOT retained in namedColumnDekCache -- those are the
                    // same byte[] instances the cache holds for future sub-batches/pages,
                    // zeroing them here would corrupt the cache for later reuse.
                    for (Map.Entry<UUID, byte[]> e : dekByEdekId.entrySet()) {
                        if (!namedColumnEdekIds.contains(e.getKey())) {
                            DekManager.zeroDek(e.getValue());
                        }
                    }
                }
            }

            lastKey = rows.get(rows.size() - 1).get(config.keyColumn());
            log.info("db_bulk_decrypt_progress job_id={} rows_done={}", jobId, rowsDoneTotal.get());
        }
    }

    /**
     * Sub-chunks a row-batch so rows.size() * columnsPerRow never exceeds
     * svc.dek-batch-max-items in one /dek/issue or /dek/unwrap call. columnsPerRow=0
     * (every column in this batch is named, or a DECRYPT batch that happens to have
     * zero source columns) means no HTTP call is made at all for this batch -- there's
     * no item cap to respect, so the whole page stays one chunk instead of being
     * artificially split down to dek-batch-max-items rows.
     */
    private List<List<Map<String, Object>>> subChunkByItemCap(List<Map<String, Object>> rows, int columnsPerRow) {
        if (columnsPerRow == 0) {
            return List.of(rows);
        }
        int maxRowsPerCall = Math.max(1, svcConfig.dekBatchMaxItems() / columnsPerRow);
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

    /** null when config.skipKeyColumnOnTarget() -- see insertRows' javadoc for what that means for the INSERT it builds. */
    private String targetKeyColumnOrNull() {
        return config.skipKeyColumnOnTarget() ? null : config.keyColumn();
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
     * a reformat that could drift from what ENCRYPT actually saw. Also reused to parse
     * a persisted checkpoint's last_key (stored as TEXT) back into the right Java
     * type -- see ClientProperties.Db's keyColumnType javadoc.
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

    /**
     * Everywhere else in this class only ever appends " FROM " + this string, so a
     * parenthesized, aliased derived table works as a drop-in for a plain table name
     * in fetchPage's SELECT and computePartitionRanges's COUNT(*)/boundary lookups --
     * no other change needed to support a query-based source. See TableRef.query's
     * javadoc for why this exists (no CREATE VIEW privilege needed) and its
     * per-page-re-execution caveat.
     */
    private static String resolveSourceFrom(ClientProperties.Db.TableRef ref) {
        if (ref.query() != null && !ref.query().isBlank()) {
            return "(" + ref.query() + ") AS src";
        }
        return qualify(ref.schema(), ref.table());
    }

    private List<Map<String, Object>> fetchPage(JdbcTemplate jdbc, String table, String keyColumn, List<String> columns,
                                                 Object afterKey, Object uptoInclusive, int limit) {
        String columnList = String.join(", ", columns);
        StringBuilder sql = new StringBuilder("SELECT ").append(keyColumn).append(", ").append(columnList)
                .append(" FROM ").append(table);
        List<Object> args = new ArrayList<>();
        List<String> conditions = new ArrayList<>();
        if (afterKey != null) {
            conditions.add(keyColumn + " > ?");
            args.add(afterKey);
        }
        if (uptoInclusive != null) {
            conditions.add(keyColumn + " <= ?");
            args.add(uptoInclusive);
        }
        if (!conditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        // ANSI SQL:2008 OFFSET/FETCH, not Postgres's LIMIT shorthand -- see class
        // javadoc. OFFSET 0 ROWS is a required, if logically redundant, part of the
        // syntax on every one of these dialects when paired with FETCH NEXT (SQL
        // Server specifically rejects FETCH NEXT without a preceding OFFSET clause,
        // even one that offsets by zero) -- "position" is entirely handled by the
        // keyset WHERE clause above; this FETCH NEXT is purely a row-count cap, the
        // same role LIMIT played before.
        sql.append(" ORDER BY ").append(keyColumn).append(" OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY");
        args.add(limit);
        return jdbc.queryForList(sql.toString(), args.toArray());
    }

    /**
     * keyColumn null means config.skipKeyColumnOnTarget() -- the key's raw value is
     * used only to drive source-side pagination (see resolveSourceFrom/fetchPage) and
     * has no place in the target INSERT at all; rows in that case already omit it
     * (see targetRow construction in encryptRange/decryptRange), so this simply
     * builds the column list/placeholders without the usual leading key column.
     */
    private void insertRows(JdbcTemplate jdbc, String table, String keyColumn, List<String> targetColumns, List<Object[]> rows) {
        if (rows.isEmpty()) {
            return;
        }
        boolean includeKey = keyColumn != null;
        String columnList = includeKey ? keyColumn + ", " + String.join(", ", targetColumns) : String.join(", ", targetColumns);
        String placeholders = String.join(", ", targetColumns.stream().map(c -> "?").toList());
        String valuesList = includeKey ? "?, " + placeholders : placeholders;
        String sql = "INSERT INTO " + table + " (" + columnList + ") VALUES (" + valuesList + ")";
        jdbc.batchUpdate(sql, rows);
    }
}
