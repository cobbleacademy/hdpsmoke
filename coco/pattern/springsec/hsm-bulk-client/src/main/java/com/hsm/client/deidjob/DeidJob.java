package com.hsm.client.deidjob;

import com.hsm.client.HsmCryptoClient;
import com.hsm.client.config.ClientProperties;
import com.hsm.client.db.DbDialect;
import com.hsm.client.deid.ColumnTransform.Method;
import com.hsm.client.deid.DeidException;
import com.hsm.client.deid.DeidPipeline;
import com.hsm.client.deid.DeidProfile;
import com.hsm.client.deid.DeidValidationException;
import com.hsm.client.deid.FieldDecryptor;
import com.hsm.client.deid.ProfileValidator;
import com.hsm.client.deid.SurrogateStore;
import com.hsm.client.deid.TokenKeyProvider;
import com.hsm.client.deid.TransformContext;
import com.hsm.client.deid.TransformRegistry;
import com.hsm.client.deidjob.sink.CsvRecordSink;
import com.hsm.client.deidjob.source.CsvRecordSource;
import com.hsm.client.deidjob.source.JdbcRecordSource;
import com.hsm.client.deidjob.source.JsonRecordSource;
import com.hsm.client.deidjob.source.RecordSource;
import com.hsm.client.deidjob.store.JdbcSurrogateStore;
import com.hsm.filestore.AdlsFileStore;
import com.hsm.filestore.AzureBlobFileStore;
import com.hsm.filestore.FileStore;
import com.hsm.filestore.LocalFileStore;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * The DEID job (DEIDENTIFICATION.md section 5): one profile, one source, one CSV.
 *
 * <ol>
 *   <li>Validate the profile against the source's columns.</li>
 *   <li>Load keys: the token key, the surrogate-table key and table, and decrypt the
 *       first value of each {@code decrypt: true} field, so a missing grant fails
 *       before anything is written.</li>
 *   <li>Stream pages: decrypt, transform, resolve surrogates, write CSV rows.</li>
 *   <li>Rename {@code .part} to the final name, write the manifest, log the audit event.</li>
 * </ol>
 *
 * <p>A failure deletes the part file: no {@code .csv} is left behind and the process
 * exits non-zero. A dry run ({@code --dry-run}) reads a sample and writes nothing,
 * not even surrogate rows.
 *
 * <p>Never logs a value. Error messages name the field and row only.
 */
public final class DeidJob {

    private static final Logger log = LoggerFactory.getLogger(DeidJob.class);
    /** The audit trail (section 10.4): one event per run. */
    private static final Logger audit = LoggerFactory.getLogger("com.hsm.client.deidjob.audit");

    /** What the job needs from hsm-core. Created only when the profile needs it. */
    public interface Hsm extends AutoCloseable {
        /** The raw named DEK, for the token and surrogate keys. The caller zeroes it. */
        byte[] unwrapDek(UUID edekId);

        /** One {@code v1.} field value, decrypted. */
        String decryptToString(String token);

        /** A whole file in the HSM encrypted file format, decrypted. */
        byte[] decryptFile(InputStream in);

        @Override
        void close();

        /** Over the embeddable client: one /dek/unwrap per edek_id, cached. */
        static Hsm of(HsmCryptoClient client) {
            return new Hsm() {
                @Override
                public byte[] unwrapDek(UUID edekId) {
                    return client.unwrapDek(edekId);
                }

                @Override
                public String decryptToString(String token) {
                    return client.decryptToString(token);
                }

                @Override
                public byte[] decryptFile(InputStream in) {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    client.decryptFile(in, out);
                    return out.toByteArray();
                }

                @Override
                public void close() {
                    client.close();
                }
            };
        }
    }

    /** What a run produced. In a dry run the files are named but not written. */
    public record Result(Path csv, Path manifestFile, Map<String, Object> manifest, Map<String, Object> review) {
    }

    private static final DateTimeFormatter RUN_ID_TIME = DateTimeFormatter.ofPattern("yyyyMMdd-HHmm").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter FILE_DATE = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    private final ClientProperties.Deid config;
    private final ClientProperties.Db db;
    private final Supplier<Hsm> hsmFactory;
    private final Supplier<DataSource> dataSourceFactory;
    private final Clock clock;
    private final SecureRandom random = CryptoServicesRegistrar.getSecureRandom();

    /**
     * @param hsmFactory        called once, only when the profile needs hsm-core
     * @param dataSourceFactory called once, only for a JDBC source or the surrogate table
     */
    public DeidJob(ClientProperties.Deid config, ClientProperties.Db db, Supplier<Hsm> hsmFactory,
                   Supplier<DataSource> dataSourceFactory, Clock clock) {
        this.config = config;
        this.db = db;
        this.hsmFactory = hsmFactory;
        this.dataSourceFactory = dataSourceFactory;
        this.clock = clock;
    }

    /** The production wiring: hsm-core through {@link HsmCryptoClient}, the database through a small Hikari pool. */
    public static DeidJob create(ClientProperties properties) {
        ClientProperties.Db db = properties.db();
        return new DeidJob(properties.deid(), db,
                () -> Hsm.of(HsmCryptoClient.fromConfig(properties.svc())),
                () -> dataSource(db),
                Clock.systemUTC());
    }

    private static DataSource dataSource(ClientProperties.Db db) {
        if (db == null || db.source() == null || db.source().jdbcUrl() == null || db.source().jdbcUrl().isBlank()) {
            throw new IllegalStateException("client.db.source.jdbc-url is required: the JDBC source and the surrogate table use it");
        }
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(db.source().jdbcUrl());
        if (db.source().username() != null && !db.source().username().isBlank()) {
            hc.setUsername(db.source().username());
        }
        if (db.source().password() != null) {
            hc.setPassword(db.source().password());
        }
        hc.setMaximumPoolSize(2);
        hc.setPoolName("deid");
        log.info("deid_pool_configured url={} username={}", db.source().jdbcUrl(), db.source().username());
        return new HikariDataSource(hc);
    }

    public Result run() {
        Instant startedAt = clock.instant();
        String runId = "deid-" + RUN_ID_TIME.format(startedAt) + "-" + HexFormat.of().formatHex(randomBytes(2));
        byte[] profileBytes = readProfile();
        String profileSha256 = sha256(profileBytes);
        DeidProfile profile = parseProfile(profileBytes);
        log.info("deid_start run_id={} profile={} version={} vendor={} basis={} dry_run={}", runId, profile.id(),
                profile.version(), profile.vendor(), profile.basis().label(), config.dryRun());

        boolean needsKeys = uses(profile, Method.TOKENIZE) || uses(profile, Method.SURROGATE);
        boolean needsDecrypt = profile.fields().stream().anyMatch(f -> f.decrypt() && f.method() != Method.DROP);
        boolean fileEncrypted = switch (config.source().type()) {
            case CSV -> config.source().csv() != null && config.source().csv().encrypted();
            case JSON -> config.source().json() != null && config.source().json().encrypted();
            case JDBC -> false;
        };

        Hsm hsm = null;
        TokenKeyProvider keys = null;
        DataSource dataSource = null;
        RecordSource source = null;
        CsvRecordSink sink = null;
        Path csvPath = null;
        try {
            if (needsKeys || needsDecrypt || fileEncrypted) {
                hsm = hsmFactory.get();
            }
            boolean jdbcSource = config.source().type() == ClientProperties.Deid.Source.Type.JDBC;
            if (jdbcSource || DeidPipeline.needsSurrogateTable(profile)) {
                dataSource = dataSourceFactory.get();
            }

            source = openSource(profile, hsm, dataSource);
            List<String> sourceColumns = source.columns();
            try {
                ProfileValidator.validate(profile, sourceColumns);
            } catch (DeidValidationException e) {
                e.problems().forEach(p -> log.error("deid_profile_problem run_id={} profile={} problem={}", runId, profile.id(), p));
                throw e;
            }
            if (source instanceof JdbcRecordSource jdbc) {
                jdbc.select(columnsToRead(profile));
            }

            // Step 2: keys and the surrogate table.
            UUID surrogateKeyEdekId = null;
            SurrogateStore store = null;
            if (needsKeys) {
                final Hsm h = hsm;
                keys = new TokenKeyProvider(h::unwrapDek);
            }
            if (DeidPipeline.needsSurrogateTable(profile)) {
                surrogateKeyEdekId = surrogateKeyEdekId();
                ClientProperties.Deid.Surrogate s = config.surrogate();
                JdbcSurrogateStore jdbcStore = new JdbcSurrogateStore(new JdbcTemplate(dataSource),
                        DbDialect.resolve(db.source().jdbcUrl(), db.source().dialect()), s.schema(), s.table(), clock);
                jdbcStore.verifyAvailable();
                store = config.dryRun() ? new DryRunSurrogateStore(jdbcStore) : jdbcStore;
            }
            TransformContext context = new TransformContext(clock, runId, keys, surrogateKeyEdekId, store, null);
            FieldDecryptor decryptor = null;
            if (needsDecrypt) {
                final Hsm h = hsm;
                decryptor = new FieldDecryptor(h::decryptToString);
            }
            DeidPipeline pipeline = new DeidPipeline(profile, new TransformRegistry(profile, context), decryptor);

            int pageSize = db != null && db.rowBatchSize() > 0 ? db.rowBatchSize() : 1000;
            List<Map<String, String>> first = source.nextPage(config.dryRun() ? Math.min(pageSize, config.sample()) : pageSize);
            pipeline.verifyDecrypt(first);
            List<String> unmarked = unmarkedCiphertext(profile, first);
            if (!unmarked.isEmpty() && !config.dryRun()) {
                throw new DeidValidationException(unmarked.stream()
                        .map(f -> "field '" + f + "' holds HSM ciphertext but isn't decrypt: true").toList());
            }

            String fileName = outputFileName(profile, runId, startedAt);
            Path root = outputRoot();
            csvPath = root.resolve(fileName);
            Path manifestPath = root.resolve(manifestName(fileName));

            if (config.dryRun()) {
                return dryRun(profile, pipeline, source, first, unmarked, runId, profileSha256, startedAt,
                        surrogateKeyEdekId, csvPath, manifestPath);
            }

            // Step 3: stream.
            DeidProfile.Output out = profile.output();
            sink = new CsvRecordSink(csvPath, out.delimiter(), out.crlf(), out.formulaGuard());
            sink.writeHeader(pipeline.header());
            long rowNumber = 1;
            List<Map<String, String>> page = first;
            while (!page.isEmpty()) {
                sink.writeRows(pipeline.process(page, rowNumber));
                rowNumber += page.size();
                page = source.nextPage(pageSize);
            }
            sink.commit();
            if (source instanceof JsonRecordSource json && !json.pathsNotSeen().isEmpty()) {
                log.warn("deid_json_paths_not_seen run_id={} profile={} paths={} (no record has them; check for a typo)",
                        runId, profile.id(), json.pathsNotSeen());
            }

            // Step 4: manifest and audit. A JSON source's columns grow as records are read.
            Map<String, Object> manifest = DeidManifest.build(profile, pipeline, new DeidManifest.Run(runId, profileSha256,
                    config.triggeredBy(), startedAt, clock.instant(), surrogateKeyEdekId, source.columns(), source.skipped(),
                    fileName, sink.sha256(), false));
            DeidManifest.write(manifestPath, manifest);
            audit.info("deid_export_completed run_id={} profile={} version={} profile_sha256={} vendor={} basis={} rows={} "
                            + "rows_skipped={} file={} file_sha256={} triggered_by={}", runId, profile.id(), profile.version(),
                    profileSha256, profile.vendor(), profile.basis().label(), pipeline.rows(), source.skipped(), fileName,
                    sink.sha256(), config.triggeredBy());
            return new Result(csvPath, manifestPath, manifest, null);
        } catch (RuntimeException e) {
            if (sink != null) {
                sink.abort();
            }
            audit.error("deid_export_failed run_id={} profile={} version={} profile_sha256={} vendor={} dry_run={} reason={}",
                    runId, profile.id(), profile.version(), profileSha256, profile.vendor(), config.dryRun(), safeReason(e));
            throw e;
        } finally {
            if (source != null) {
                source.close();
            }
            if (keys != null) {
                keys.close();
            }
            if (hsm != null) {
                hsm.close();
            }
            if (dataSource instanceof AutoCloseable c) {
                try {
                    c.close();
                } catch (Exception ignored) {
                    // closing the pool on the way out
                }
            }
        }
    }

    // ---- dry run (section 10.3) ----

    private Result dryRun(DeidProfile profile, DeidPipeline pipeline, RecordSource source, List<Map<String, String>> first,
                          List<String> unmarked, String runId, String profileSha256, Instant startedAt,
                          UUID surrogateKeyEdekId, Path csvPath, Path manifestPath) {
        List<String> header = pipeline.header();
        List<Set<String>> distinct = new ArrayList<>();
        header.forEach(h -> distinct.add(new HashSet<>()));
        int read = 0;
        List<Map<String, String>> page = first;
        while (!page.isEmpty()) {
            for (List<String> row : pipeline.process(page, read + 1L)) {
                for (int i = 0; i < row.size(); i++) {
                    if (row.get(i) != null && !row.get(i).isEmpty()) {
                        distinct.get(i).add(row.get(i));
                    }
                }
            }
            read += page.size();
            if (read >= config.sample()) {
                break;
            }
            page = source.nextPage(config.sample() - read);
        }
        Map<String, Object> manifest = DeidManifest.build(profile, pipeline, new DeidManifest.Run(runId, profileSha256,
                config.triggeredBy(), startedAt, null, surrogateKeyEdekId, source.columns(), source.skipped(),
                csvPath.getFileName().toString(), null, true));

        List<Map<String, Object>> fields = new ArrayList<>();
        List<DeidPipeline.FieldReport> reports = pipeline.report();
        for (int i = 0; i < reports.size(); i++) {
            var r = reports.get(i);
            Map<String, Object> f = new LinkedHashMap<>();
            f.put("source", r.source());
            f.put("output", r.output());
            f.put("method", r.method());
            f.put("phi", r.phi());
            f.put("null_rate", r.values() == 0 ? 0.0 : Math.round(1000.0 * r.empty() / r.values()) / 1000.0);
            f.put("distinct_in_sample", distinct.get(i).size());
            fields.add(f);
        }
        Map<String, Object> review = new LinkedHashMap<>();
        review.put("sample_rows", pipeline.rows());
        review.put("fields", fields);
        review.put("passthrough", pipeline.passthroughFields());
        review.put("dropped_by_default", pipeline.droppedByDefault(source.columns()));
        review.put("ciphertext_not_marked_decrypt", unmarked);
        if (source instanceof JsonRecordSource json) {
            review.put("paths_not_seen", json.pathsNotSeen());
        }

        Map<String, Object> printed = new LinkedHashMap<>();
        printed.put("manifest", manifest);
        printed.put("review", review);
        System.out.println(DeidManifest.toJson(printed));
        log.info("deid_dry_run_complete run_id={} profile={} sample_rows={} unmarked_ciphertext_fields={}", runId,
                profile.id(), pipeline.rows(), unmarked.size());
        return new Result(csvPath, manifestPath, manifest, review);
    }

    /**
     * A dry run must write nothing: reads go to the table, inserts stay in memory so the
     * sample is still consistent (one surrogate per value).
     */
    static final class DryRunSurrogateStore implements SurrogateStore {
        private final SurrogateStore delegate;
        private final Map<String, String> pending = new HashMap<>();
        private final Set<String> taken = new HashSet<>();

        DryRunSurrogateStore(SurrogateStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Map<String, String> find(String scope, String domain, List<byte[]> lookupHashes) {
            Map<String, String> found = new HashMap<>(delegate.find(scope, domain, lookupHashes));
            for (byte[] h : lookupHashes) {
                String hex = HexFormat.of().formatHex(h);
                String p = pending.get(scope + "|" + domain + "|" + hex);
                if (p != null) {
                    found.put(hex, p);
                }
            }
            return found;
        }

        @Override
        public InsertResult insertIfAbsent(String scope, String domain, byte[] lookupHash, byte[] valueEnc, UUID keyEdekId,
                                           String surrogate, String runId) {
            String key = scope + "|" + domain + "|" + HexFormat.of().formatHex(lookupHash);
            if (pending.containsKey(key)) {
                return InsertResult.ROW_EXISTS;
            }
            if (!taken.add(scope + "|" + domain + "|" + surrogate)) {
                return InsertResult.SURROGATE_TAKEN;
            }
            pending.put(key, surrogate);
            return InsertResult.INSERTED;
        }
    }

    // ---- pieces ----

    private RecordSource openSource(DeidProfile profile, Hsm hsm, DataSource dataSource) {
        List<String> names = profile.fields().stream().map(DeidProfile.Field::name).toList();
        if (config.source().type() == ClientProperties.Deid.Source.Type.JDBC) {
            ClientProperties.Db.TableRef ref = db.source();
            String table = ref.table() == null || ref.table().isBlank() ? null
                    : (ref.schema() == null || ref.schema().isBlank() ? ref.table() : ref.schema() + "." + ref.table());
            return new JdbcRecordSource(new JdbcTemplate(dataSource), table, ref.query(), db.keyColumn(), names);
        }
        if (config.source().type() == ClientProperties.Deid.Source.Type.JSON) {
            ClientProperties.Deid.Json json = config.source().json();
            if (json == null) {
                throw new IllegalStateException("client.deid.source.json.store and .path are required for a JSON source");
            }
            InputStream in = openFile("json", json.store(), json.path(), json.encrypted(), hsm);
            JsonRecordSource.Layout layout = json.layout() == ClientProperties.Deid.Json.Layout.ARRAY
                    ? JsonRecordSource.Layout.ARRAY : JsonRecordSource.Layout.LINES;
            try {
                return new JsonRecordSource(in, Charset.forName(json.encoding()), layout,
                        json.onBadRow() == ClientProperties.Deid.Csv.OnBadRow.SKIP, names, columnsToRead(profile));
            } catch (RuntimeException e) {
                closeQuietly(in);
                throw e;
            }
        }
        ClientProperties.Deid.Csv csv = config.source().csv();
        if (csv == null) {
            throw new IllegalStateException("client.deid.source.csv.store and .path are required for a CSV source");
        }
        InputStream in = openFile("csv", csv.store(), csv.path(), csv.encrypted(), hsm);
        try {
            return new CsvRecordSource(in, Charset.forName(csv.encoding()), csv.delimiter().charAt(0), csv.quote().charAt(0),
                    csv.onBadRow() == ClientProperties.Deid.Csv.OnBadRow.SKIP);
        } catch (RuntimeException e) {
            closeQuietly(in);
            throw e;
        }
    }

    /** Opens a CSV or JSON source file; a whole HSM-encrypted file is decrypted in memory first. */
    private static InputStream openFile(String kind, ClientProperties.File.StoreRef ref, String path, boolean encrypted,
                                        Hsm hsm) {
        if (ref == null || path == null || path.isBlank()) {
            throw new IllegalStateException("client.deid.source." + kind + ".store and .path are required for a "
                    + kind.toUpperCase() + " source");
        }
        FileStore store = switch (ref.type()) {
            case LOCAL -> new LocalFileStore(ref.root());
            case ADLS -> new AdlsFileStore(ref.root(), ref.accountKey());
            case AZURE_BLOB -> new AzureBlobFileStore(ref.root(), ref.accountKey());
        };
        InputStream in = store.openRead(path);
        if (encrypted) {
            try (InputStream enc = in) {
                in = new ByteArrayInputStream(hsm.decryptFile(enc));
            } catch (IOException e) {
                throw new UncheckedIOException("reading the encrypted " + kind.toUpperCase() + " source failed", e);
            }
        }
        return in;
    }

    private static void closeQuietly(InputStream in) {
        try {
            in.close();
        } catch (IOException ignored) {
            // already failing
        }
    }

    /** Exported fields and the fields their options read ({@code as-of}); nothing else is selected. */
    static Set<String> columnsToRead(DeidProfile profile) {
        Set<String> cols = new LinkedHashSet<>();
        for (DeidProfile.Field f : profile.fields()) {
            if (f.method() == Method.DROP) {
                continue;
            }
            cols.add(f.name());
            Object asOf = f.options().get("as-of");
            if (asOf != null) {
                cols.add(asOf.toString());
            }
        }
        return cols;
    }

    /** Listed, exported fields whose sample values are HSM ciphertext but that aren't {@code decrypt: true}. */
    static List<String> unmarkedCiphertext(DeidProfile profile, List<Map<String, String>> sample) {
        List<String> out = new ArrayList<>();
        for (DeidProfile.Field f : profile.fields()) {
            if (f.decrypt() || f.method() == Method.DROP) {
                continue;
            }
            for (Map<String, String> r : sample) {
                if (FieldDecryptor.looksEncrypted(r.get(f.name()))) {
                    out.add(f.name());
                    break;
                }
            }
        }
        return out;
    }

    private UUID surrogateKeyEdekId() {
        String id = config.surrogate().keyEdekId();
        if (id == null || id.isBlank()) {
            throw new IllegalStateException("the profile uses surrogate: client.deid.surrogate.key-edek-id "
                    + "(DEID_SURROGATE_KEY_EDEK_ID) is required; create the key with --deid.init-key");
        }
        try {
            return UUID.fromString(id.strip());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("client.deid.surrogate.key-edek-id is not a UUID");
        }
    }

    private byte[] readProfile() {
        if (config.profile() == null || config.profile().isBlank()) {
            throw new IllegalStateException("client.deid.profile (DEID_PROFILE) is required");
        }
        try {
            return Files.readAllBytes(Path.of(config.profile()));
        } catch (IOException e) {
            throw new UncheckedIOException("can't read the profile " + config.profile(), e);
        }
    }

    static DeidProfile parseProfile(byte[] yaml) {
        Object doc = new Yaml(new SafeConstructor(new LoaderOptions()))
                .load(new String(yaml, StandardCharsets.UTF_8));
        if (!(doc instanceof Map<?, ?> map)) {
            throw new DeidValidationException(List.of("the profile is not a YAML mapping"));
        }
        @SuppressWarnings("unchecked")
        Map<String, ?> m = (Map<String, ?>) map;
        return DeidProfile.fromMap(m);
    }

    private Path outputRoot() {
        String root = config.output().root();
        if (root == null || root.isBlank()) {
            throw new IllegalStateException("client.deid.output.root (DEID_OUTPUT_ROOT) is required");
        }
        return Path.of(root);
    }

    /** {@code output.file} with {@code {yyyyMMdd}}, {@code {run}} and {@code {profile}} filled in. */
    String outputFileName(DeidProfile profile, String runId, Instant startedAt) {
        String name = profile.output().file()
                .replace("{yyyyMMdd}", FILE_DATE.format(startedAt))
                .replace("{run}", runId)
                .replace("{profile}", profile.id());
        if (name.contains("/") || name.contains("\\") || name.startsWith(".") || name.contains("{")) {
            throw new IllegalStateException("output.file must be a plain file name with only the {yyyyMMdd}, {run} and "
                    + "{profile} placeholders");
        }
        return name;
    }

    static String manifestName(String csvName) {
        String base = csvName.toLowerCase().endsWith(".csv") ? csvName.substring(0, csvName.length() - 4) : csvName;
        return base + ".manifest.json";
    }

    private static boolean uses(DeidProfile profile, Method method) {
        return profile.fields().stream().anyMatch(f -> f.method() == method);
    }

    /**
     * The failure reason for the audit log. Messages from this package and the deid
     * library never carry values; anything else is reduced to its class name.
     */
    static String safeReason(Throwable e) {
        if (e instanceof DeidValidationException v) {
            return v.problems().size() + " profile problem(s)";
        }
        if (e instanceof DeidException || e instanceof IllegalStateException || e instanceof IllegalArgumentException) {
            return e.getMessage();
        }
        return e.getClass().getSimpleName();
    }

    private byte[] randomBytes(int n) {
        byte[] b = new byte[n];
        random.nextBytes(b);
        return b;
    }

    static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
