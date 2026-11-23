package com.hsm.client.deidjob;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.hsm.client.config.ClientProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hsm.client.deidjob.DeidJobTestSupport.CLOCK_2026;
import static com.hsm.client.deidjob.DeidJobTestSupport.SURROGATE_KEY_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The Java side of the Java/Python cross-check (DEIDENTIFICATION.md section 17.2):
 * the job on {@code deid-crosscheck/input.json} with each profile in
 * {@code deid-crosscheck/profiles} must write exactly {@code deid-crosscheck/expected}.
 * The Python job ({@code hsm-databricks-udf/tests/deid/test_crosscheck.py}) must write
 * the same files from the same input.
 *
 * <p>Surrogates are random, so {@code surrogates.json} holds the rows a first run
 * created; both sides load them into an empty table first, so every member already
 * has its surrogate and both files match byte for byte.
 *
 * <p>To regenerate the expected files after a deliberate change to the methods (which
 * changes every vendor's files, so with the privacy officer's agreement), run with
 * {@code -Ddeid.crosscheck.write=true}.
 */
class DeidCrossCheckTest {

    private static final Path DIR = Path.of("../hsm-crypto-client/src/test/resources/deid-crosscheck");
    private static final List<String> PROFILES = List.of("zen", "acme", "types");
    private static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    static {
        // As ClientApplication.main does first: the job takes its randomness from BC-FIPS.
        com.hsm.crypto.FipsBootstrap.register();
    }

    @TempDir
    Path tmp;

    @Test
    void theJobWritesTheExpectedFiles() throws Exception {
        boolean write = Boolean.getBoolean("deid.crosscheck.write");
        JsonNode input = JSON.readTree(Files.readString(DIR.resolve("input.json")));
        Path seedFile = DIR.resolve("surrogates.json");
        if (write) {
            writeSeeds(input, seedFile);
        }
        JsonNode seeds = JSON.readTree(Files.readString(seedFile));
        for (String name : PROFILES) {
            DeidJob.Result r = run(input, name, seeds);
            Path expectedCsv = DIR.resolve("expected/" + name + ".csv");
            Path expectedManifest = DIR.resolve("expected/" + name + ".manifest.json");
            Map<String, Object> comparable = comparable(r.manifest());
            if (write) {
                Files.write(expectedCsv, Files.readAllBytes(r.csv()));
                Files.writeString(expectedManifest, JSON.writeValueAsString(comparable) + "\n");
            } else {
                assertEquals(Files.readString(expectedCsv, StandardCharsets.UTF_8),
                        Files.readString(r.csv(), StandardCharsets.UTF_8), name + ".csv");
                // through text, so a long and an int of the same value compare equal
                assertEquals(JSON.readTree(Files.readString(expectedManifest)),
                        JSON.readTree(JSON.writeValueAsString(comparable)), name + ".manifest.json");
            }
        }
    }

    /** One run of a profile on a fresh database: the input table, and the surrogate table with {@code seeds} (or empty). */
    private DeidJob.Result run(JsonNode input, String name, JsonNode seeds) {
        DriverManagerDataSource ds = DeidJobTestSupport.h2();
        JdbcTemplate jdbc = new JdbcTemplate(ds);
        createTable(jdbc, input);
        DeidJobTestSupport.createSurrogateTable(jdbc);
        if (seeds != null) {
            seed(jdbc, seeds);
        }
        lastJdbc = jdbc;
        DeidJobTestSupport.FakeHsm hsm = new DeidJobTestSupport.FakeHsm();
        Path out = tmp.resolve(name + (seeds == null ? "-unseeded" : ""));
        ClientProperties.Deid deid = new ClientProperties.Deid(DIR.resolve("profiles/" + name + ".yml").toString(),
                null, new ClientProperties.Deid.Output(out.toString()),
                new ClientProperties.Deid.Surrogate(SURROGATE_KEY_ID.toString(), null, null), false, 0, "crosscheck");
        ClientProperties.Db db = new ClientProperties.Db(new ClientProperties.Db.TableRef("jdbc:h2:mem:x", "sa", "",
                null, "giftcards", null, null), null, "id", null, false, List.of(), List.of(), 1000, null, 1);
        return new DeidJob(deid, db, () -> hsm, () -> ds, CLOCK_2026).run();
    }

    private JdbcTemplate lastJdbc;

    /** Runs every profile on an empty surrogate table and keeps the rows created, as {@code surrogates.json}. */
    private void writeSeeds(JsonNode input, Path seedFile) throws Exception {
        List<Map<String, Object>> created = new ArrayList<>();
        for (String name : PROFILES) {
            run(input, name, null);
            created.addAll(lastJdbc.query("SELECT scope, domain, lookup_hash, value_enc, surrogate FROM deid_surrogate "
                    + "ORDER BY domain, surrogate", (rs, i) -> {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("scope", rs.getString(1));
                row.put("domain", rs.getString(2));
                row.put("lookup_hash", HexFormat.of().formatHex(rs.getBytes(3)));
                row.put("value_enc", HexFormat.of().formatHex(rs.getBytes(4)));
                row.put("surrogate", rs.getString(5));
                return row;
            }));
        }
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("description", "deid_surrogate rows a first run of the cross-check created (key: "
                + "DeidJobTestSupport.dek(3), edek_id " + SURROGATE_KEY_ID + "). Loaded before each run on both "
                + "sides, so the surrogates in the expected files are reused, not created.");
        doc.put("key_edek_id", SURROGATE_KEY_ID.toString());
        doc.put("rows", created);
        Files.writeString(seedFile, JSON.writeValueAsString(doc) + "\n");
    }

    /** The manifest without what differs between runs (run id, times, file hash covered by the CSV itself). */
    static Map<String, Object> comparable(Map<String, Object> manifest) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("profile", manifest.get("profile"));
        m.put("keys", manifest.get("keys"));
        m.put("fields", manifest.get("fields"));
        m.put("dropped_by_default", manifest.get("dropped_by_default"));
        if (manifest.containsKey("surrogates")) {
            m.put("surrogates", manifest.get("surrogates"));
        }
        if (manifest.containsKey("decrypt")) {
            m.put("decrypt", manifest.get("decrypt"));
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> out = new LinkedHashMap<>((Map<String, Object>) manifest.get("output"));
        m.put("output", out);
        return m;
    }

    private static void createTable(JdbcTemplate jdbc, JsonNode input) {
        List<String> defs = new ArrayList<>();
        List<String> casts = new ArrayList<>();
        for (JsonNode c : input.get("columns")) {
            defs.add(c.get("name").asText() + " " + c.get("type").asText());
            casts.add("CAST(? AS " + c.get("type").asText() + ")");
        }
        jdbc.execute("CREATE TABLE giftcards (" + String.join(", ", defs) + ")");
        String insert = "INSERT INTO giftcards VALUES (" + String.join(", ", casts) + ")";
        for (JsonNode row : input.get("rows")) {
            Object[] values = new Object[row.size()];
            for (int i = 0; i < row.size(); i++) {
                values[i] = row.get(i).isNull() ? null : row.get(i).asText();
            }
            jdbc.update(insert, values);
        }
    }

    private static void seed(JdbcTemplate jdbc, JsonNode seeds) {
        for (JsonNode r : seeds.get("rows")) {
            jdbc.update("INSERT INTO deid_surrogate (scope, domain, lookup_hash, value_enc, key_edek_id, surrogate, "
                            + "created_run_id) VALUES (?, ?, ?, ?, ?, ?, ?)", r.get("scope").asText(), r.get("domain").asText(),
                    HexFormat.of().parseHex(r.get("lookup_hash").asText()), HexFormat.of().parseHex(r.get("value_enc").asText()),
                    java.util.UUID.fromString(seeds.get("key_edek_id").asText()), r.get("surrogate").asText(), "seed");
        }
    }
}
