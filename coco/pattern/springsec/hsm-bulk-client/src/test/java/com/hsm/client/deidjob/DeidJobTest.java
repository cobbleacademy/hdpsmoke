package com.hsm.client.deidjob;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.hsm.client.config.ClientProperties;
import com.hsm.client.deid.DeidException;
import com.hsm.client.deid.DeidValidationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.hsm.client.deidjob.DeidJobTestSupport.CLOCK_2026;
import static com.hsm.client.deidjob.DeidJobTestSupport.SURROGATE_KEY_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The DEID job end to end on H2: the gift-card table (two encrypted columns) exported
 * for a reporting vendor (Safe Harbor, surrogates) and a fulfillment vendor
 * (minimum-necessary, tokens and masks), from a table and from a CSV file.
 */
class DeidJobTest {

    private static final String ZEN = """
            id: zen-reporting
            version: 1
            vendor: zen-analytics
            hipaa-basis: safe-harbor
            surrogate-scope: zen-analytics
            fields:
              - { name: member_id,   phi: member-id,   method: surrogate, domain: member }
              - { name: card_number, phi: card-number, decrypt: true, method: surrogate, domain: card, normalize: digits }
              - { name: dob,         phi: birth-date,  method: generalize, to: birth-year }
              - { name: issued_date, phi: date,        method: generalize, to: year }
              - { name: zip,         phi: zip,         method: generalize, to: zip3 }
              - { name: email,       phi: email,       method: drop }
              - { name: amount,                        method: passthrough }
              - { name: status,                        method: passthrough }
            output:
              file: "giftcards_zen_{yyyyMMdd}.csv"
              rename: { member_id: member_ref }
            """;

    private static final String ACME = """
            id: acme-fulfillment
            version: 1
            vendor: acme-rewards
            hipaa-basis: minimum-necessary
            token-key-edek-id: 6f1c0000-0000-4000-8000-000000000001
            token-domain: member
            fields:
              - { name: member_id,   phi: member-id,   method: tokenize }
              - { name: first_name,  phi: name,        method: passthrough }
              - { name: email,       phi: email,       decrypt: true, method: mask, keep: domain, keep-first: 1 }
              - { name: card_number, phi: card-number, decrypt: true, method: mask, keep-last: 4 }
              - { name: amount,                        method: passthrough }
            output: { file: "giftcards_acme_{yyyyMMdd}.csv", formula-guard: false }
            """;

    /** Only drop/mask/generalize/passthrough on plain columns: never calls hsm-core. */
    private static final String PLAIN = """
            id: plain
            version: 1
            hipaa-basis: minimum-necessary
            fields:
              - { name: first_name, phi: name, method: mask, keep-first: 1 }
              - { name: zip,        phi: zip,  method: generalize, to: zip3 }
              - { name: amount,                method: passthrough }
            output: { file: "plain_{run}.csv" }
            """;

    private static final List<String> SENSITIVE = List.of("M1234567", "M7654321", "Jane", "jane.doe", "raj.p",
            "6035712345678901", "6035719876543210", "4821", "1985-03-14", "60614");

    @TempDir
    Path dir;

    private DriverManagerDataSource ds;
    private JdbcTemplate jdbc;
    private DeidJobTestSupport.FakeHsm hsm;
    private int hsmCreated;
    private ListAppender<ILoggingEvent> logs;
    private Logger root;

    @BeforeEach
    void setUp() {
        ds = DeidJobTestSupport.h2();
        jdbc = new JdbcTemplate(ds);
        hsm = new DeidJobTestSupport.FakeHsm();
        DeidJobTestSupport.createGiftcards(jdbc, hsm, true);
        DeidJobTestSupport.createSurrogateTable(jdbc);
        root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logs = new ListAppender<>();
        logs.start();
        root.addAppender(logs);
    }

    @AfterEach
    void tearDown() {
        if (root != null) {
            root.detachAppender(logs);
        }
    }

    private Path profile(String name, String yaml) throws IOException {
        Path p = dir.resolve(name + ".yml");
        Files.writeString(p, yaml);
        return p;
    }

    private static ClientProperties.Db db(int pageSize) {
        return new ClientProperties.Db(
                new ClientProperties.Db.TableRef("jdbc:h2:mem:x", "sa", "", null, "giftcards", null, null),
                null, "id", null, false, List.of(), List.of(), pageSize, null, 1);
    }

    private ClientProperties.Deid deid(Path profile, ClientProperties.Deid.Source source, boolean dryRun) {
        return new ClientProperties.Deid(profile.toString(), source, new ClientProperties.Deid.Output(dir.resolve("out").toString()),
                new ClientProperties.Deid.Surrogate(SURROGATE_KEY_ID.toString(), null, null), dryRun, 100, "pipeline-42");
    }

    private DeidJob job(ClientProperties.Deid deid, int pageSize) {
        return new DeidJob(deid, db(pageSize), () -> {
            hsmCreated++;
            return hsm;
        }, () -> ds, CLOCK_2026);
    }

    private DeidJob.Result runZen(int pageSize) throws IOException {
        return job(deid(profile("zen", ZEN), null, false), pageSize).run();
    }

    private static List<String> lines(Path p) throws IOException {
        return Files.readAllLines(p, StandardCharsets.UTF_8);
    }

    private List<String> outFiles() throws IOException {
        Path out = dir.resolve("out");
        if (!Files.exists(out)) {
            return List.of();
        }
        try (Stream<Path> s = Files.list(out)) {
            return s.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }

    @Test
    void safeHarborExportFromATable() throws IOException {
        DeidJob.Result r = runZen(2);

        assertEquals(List.of("giftcards_zen_20261120.csv", "giftcards_zen_20261120.manifest.json"), outFiles());
        List<String> csv = lines(r.csv());
        assertEquals("member_ref,card_number,dob,issued_date,zip,amount,status", csv.get(0));
        assertEquals(5, csv.size());
        String[] jane = csv.get(1).split(",", -1);
        String[] raj = csv.get(2).split(",", -1);
        String[] janeAgain = csv.get(3).split(",", -1);
        String[] empty = csv.get(4).split(",", -1);
        assertTrue(jane[0].matches("S-[0-9A-Z]{5}-[0-9A-Z]{5}"), jane[0]);
        assertEquals(jane[0], janeAgain[0], "same member, same surrogate");
        assertEquals(jane[1], janeAgain[1], "same card, same surrogate");
        assertNotEquals(jane[0], raj[0]);
        assertEquals(List.of("1985", "2026", "606", "25.00", "ACTIVE"), List.of(jane).subList(2, 7));
        assertEquals(List.of("<=1936", "2026", "000", "100.00", "REDEEMED"), List.of(raj).subList(2, 7),
                "over 89 is capped; 036 is a restricted ZIP3");
        assertEquals("'-5.00", janeAgain[5], "formula guard is on by default");
        assertTrue(empty[0].startsWith("S-"), "a member with no other data still gets a surrogate");
        assertEquals(List.of("", "", "", "", "10.00", "ACTIVE"), List.of(empty).subList(1, 7), "nulls stay empty");

        for (String line : csv) {
            for (String s : SENSITIVE) {
                assertFalse(line.contains(s), "CSV contains " + s);
            }
        }

        Map<String, Object> m = r.manifest();
        assertEquals("pipeline-42", m.get("triggered_by"));
        @SuppressWarnings("unchecked")
        Map<String, Object> out = (Map<String, Object>) m.get("output");
        assertEquals(4L, out.get("rows_out"));
        assertEquals("giftcards_zen_20261120.csv", out.get("file"));
        assertEquals(List.of("id", "first_name", "card_pin"), m.get("dropped_by_default"));
        // Counted per page: rows 1-2, then rows 3-4, where Jane's member and card already exist.
        assertEquals(Map.of("new", 5L, "existing", 2L), m.get("surrogates"), "3 members + 2 cards are new");
        @SuppressWarnings("unchecked")
        Map<String, Object> decrypt = (Map<String, Object>) m.get("decrypt");
        assertEquals(List.of("card_number"), decrypt.get("fields"));
        assertEquals(1, decrypt.get("distinct_edek_ids"));
        String manifestText = Files.readString(r.manifestFile());
        for (String s : SENSITIVE) {
            assertFalse(manifestText.contains(s), "manifest contains " + s);
        }
        assertFalse(manifestText.contains(jane[0]), "manifest contains a surrogate");
        assertEquals(5, jdbc.queryForObject("SELECT COUNT(*) FROM deid_surrogate", Integer.class));
        assertTrue(hsm.closed);
    }

    @Test
    void theNextRunReusesTheSurrogates() throws IOException {
        List<String> first = lines(runZen(100).csv());
        Files.move(dir.resolve("out"), dir.resolve("out-1"));
        DeidJob.Result second = runZen(3);
        assertEquals(first, lines(second.csv()));
        // Distinct values per page: rows 1-3 (2 members, 2 cards), then row 4 (1 member).
        assertEquals(Map.of("new", 0L, "existing", 5L), second.manifest().get("surrogates"));
    }

    @Test
    void minimumNecessaryExportWithTokensAndMasks() throws IOException {
        DeidJob.Result r = job(deid(profile("acme", ACME), null, false), 10).run();
        List<String> csv = lines(r.csv());
        assertEquals("member_id,first_name,email,card_number,amount", csv.get(0));
        String[] jane = csv.get(1).split(",", -1);
        assertTrue(jane[0].startsWith("tk_"), jane[0]);
        assertEquals("Jane", jane[1]);
        assertEquals("j*******@mail.com", jane[2]);
        assertEquals("************8901", jane[3]);
        assertEquals(jane[0], csv.get(3).split(",", -1)[0]);
        assertEquals("-5.00", csv.get(3).split(",", -1)[4], "formula guard off in this profile");
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM deid_surrogate", Integer.class));
    }

    @Test
    void aCsvSourceGivesTheSameRowsAsTheTable() throws IOException {
        Path in = dir.resolve("in");
        Files.createDirectories(in);
        StringBuilder text = new StringBuilder("﻿id,member_id,first_name,email,card_number,amount,extra\n");
        text.append("1,M1234567,Jane,").append(hsm.encrypt("jane.doe@mail.com")).append(',')
                .append(hsm.encrypt("6035712345678901")).append(",25.00,x\n");
        text.append("2,M7654321,\"Raj, P\",").append(hsm.encrypt("raj.p@corp.org")).append(',')
                .append(hsm.encrypt("6035719876543210")).append(",100.00,y\n");
        Files.writeString(in.resolve("giftcards.csv"), text);
        ClientProperties.Deid.Source source = new ClientProperties.Deid.Source(ClientProperties.Deid.Source.Type.CSV,
                new ClientProperties.Deid.Csv(new ClientProperties.File.StoreRef(ClientProperties.File.StoreType.LOCAL,
                        in.toString(), null), "giftcards.csv", null, null, null, null, false), null);

        List<String> fromCsv = lines(job(deid(profile("acme", ACME), source, false), 10).run().csv());
        Files.move(dir.resolve("out"), dir.resolve("out-csv"));
        List<String> fromTable = lines(job(deid(profile("acme", ACME), null, false), 10).run().csv());

        assertEquals(fromTable.subList(0, 2), fromCsv.subList(0, 2));
        assertTrue(fromCsv.get(2).startsWith(fromTable.get(2).split(",")[0] + ",\"Raj, P\",r****@corp.org,"), fromCsv.get(2));
    }

    @Test
    void aDryRunWritesNothingAndFlagsUnmarkedCiphertext() throws IOException {
        String unmarked = ZEN.replace("decrypt: true, method: surrogate, domain: card, normalize: digits",
                "method: surrogate, domain: card");
        PrintStream stdout = System.out;
        ByteArrayOutputStream printed = new ByteArrayOutputStream();
        DeidJob.Result r;
        try {
            System.setOut(new PrintStream(printed, true, StandardCharsets.UTF_8));
            r = job(deid(profile("zen", unmarked), null, true), 2).run();
        } finally {
            System.setOut(stdout);
        }
        assertEquals(List.of(), outFiles());
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM deid_surrogate", Integer.class), "no surrogate rows written");
        assertEquals(List.of("card_number"), r.review().get("ciphertext_not_marked_decrypt"));
        assertEquals(4L, r.review().get("sample_rows"));
        assertEquals(true, r.manifest().get("dry_run"));
        String text = printed.toString(StandardCharsets.UTF_8);
        assertTrue(text.contains("\"null_rate\""), text);
        assertTrue(text.contains("\"distinct_in_sample\""));
        for (String s : SENSITIVE) {
            assertFalse(text.contains(s), "dry run printed " + s);
        }
    }

    @Test
    void aRealRunRefusesUnmarkedCiphertext() throws IOException {
        String unmarked = ACME.replace("decrypt: true, method: mask, keep-last: 4", "method: mask, keep-last: 4");
        DeidValidationException e = assertThrows(DeidValidationException.class,
                () -> job(deid(profile("acme", unmarked), null, false), 10).run());
        assertTrue(e.problems().get(0).contains("card_number"));
        assertEquals(List.of(), outFiles());
    }

    @Test
    void aProfileProblemFailsBeforeAnythingIsRead() throws IOException {
        String bad = ZEN.replace("- { name: email,       phi: email,       method: drop }",
                "- { name: first_name, phi: name, method: passthrough }");
        DeidValidationException e = assertThrows(DeidValidationException.class,
                () -> job(deid(profile("zen", bad), null, false), 10).run());
        assertTrue(e.problems().stream().anyMatch(p -> p.contains("first_name")), e.problems().toString());
        assertEquals(List.of(), outFiles());
        assertTrue(logs.list.stream().anyMatch(ev -> ev.getFormattedMessage().startsWith("deid_export_failed")));
    }

    @Test
    void aBadValueMidRunLeavesNoCsvAndNamesFieldAndRowOnly() throws IOException {
        jdbc.update("UPDATE giftcards SET card_number = ? WHERE id = 3", "6035712345678901");
        DeidException e = assertThrows(DeidException.class, () -> runZen(2));
        assertTrue(e.getMessage().contains("card_number") && e.getMessage().contains("row 3"), e.getMessage());
        assertFalse(e.getMessage().contains("6035712345678901"));
        assertEquals(List.of(), outFiles(), "no .csv and no .part");
    }

    @Test
    void aMissingGrantFailsAtValidationBeforeWriting() throws IOException {
        hsm.deks.remove(DeidJobTestSupport.COLUMN_DEK_ID);
        DeidException e = assertThrows(DeidException.class, () -> runZen(2));
        assertTrue(e.getMessage().contains("grant"), e.getMessage());
        assertEquals(List.of(), outFiles());
    }

    @Test
    void aMissingSurrogateTableOrKeyFailsClearly() throws IOException {
        jdbc.execute("DROP TABLE deid_surrogate");
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> runZen(2));
        assertTrue(e.getMessage().contains("deid-surrogate-postgres.sql"), e.getMessage());

        ClientProperties.Deid noKey = new ClientProperties.Deid(profile("zen", ZEN).toString(), null,
                new ClientProperties.Deid.Output(dir.resolve("out").toString()), null, false, 0, null);
        IllegalStateException k = assertThrows(IllegalStateException.class, () -> job(noKey, 10).run());
        assertTrue(k.getMessage().contains("key-edek-id"));
    }

    @Test
    void aPlainProfileNeverCallsHsmCore() throws IOException {
        DeidJob job = new DeidJob(deid(profile("plain", PLAIN), null, false), db(10), () -> {
            throw new AssertionError("hsm-core called");
        }, () -> ds, CLOCK_2026);
        DeidJob.Result r = job.run();
        assertTrue(r.csv().getFileName().toString().matches("plain_deid-20261120-1530-[0-9a-f]{4}\\.csv"));
        assertEquals("J***,606,25.00", lines(r.csv()).get(1));
    }

    @Test
    void anExistingFileIsNeverOverwritten() throws IOException {
        runZen(10);
        assertThrows(IllegalStateException.class, () -> runZen(10));
        assertEquals(2, outFiles().size());
    }

    @Test
    void noSensitiveValueIsLogged() throws IOException {
        runZen(2);
        job(deid(profile("acme", ACME), null, false), 2).run();
        assertTrue(logs.list.stream().anyMatch(ev -> ev.getFormattedMessage().startsWith("deid_export_completed")));
        for (ILoggingEvent ev : logs.list) {
            String msg = ev.getFormattedMessage();
            for (String s : SENSITIVE) {
                assertFalse(msg.contains(s), "logged " + s + ": " + msg);
            }
        }
    }

    private static final String ZEN_JSON = """
            id: zen-members-json
            version: 1
            vendor: zen-analytics
            hipaa-basis: safe-harbor
            surrogate-scope: zen-analytics
            fields:
              - { name: member_id,           phi: member-id,   method: surrogate, domain: member }
              - { name: name.first,          phi: name,        method: drop }
              - { name: dob,                 phi: birth-date,  method: generalize, to: birth-year }
              - { name: address.zip,         phi: zip,         method: generalize, to: zip3 }
              - { name: "cards[].card_number", phi: card-number, decrypt: true, method: surrogate, domain: card }
              - { name: "cards[].pin",                           method: drop }
              - { name: "cards[].amount",                        method: passthrough }
            output:
              file: "members_zen_{yyyyMMdd}.csv"
              rename: { member_id: member_ref }
            """;

    private ClientProperties.Deid.Source jsonSource(String text, ClientProperties.Deid.Json.Layout layout) throws IOException {
        Path in = dir.resolve("in");
        Files.createDirectories(in);
        Files.writeString(in.resolve("members.json"), text);
        return new ClientProperties.Deid.Source(ClientProperties.Deid.Source.Type.JSON, null,
                new ClientProperties.Deid.Json(new ClientProperties.File.StoreRef(ClientProperties.File.StoreType.LOCAL,
                        in.toString(), null), "members.json", layout, null, null, false));
    }

    private String membersJsonl() {
        return "{\"member_id\":\"M1234567\",\"name\":{\"first\":\"Jane\",\"last\":\"Doe\"},\"dob\":\"1985-03-14\","
                + "\"address\":{\"zip\":\"60614-2231\"},\"cards\":["
                + "{\"card_number\":\"" + hsm.encrypt("6035712345678901") + "\",\"pin\":\"4821\",\"amount\":25.00},"
                + "{\"card_number\":\"" + hsm.encrypt("6035719876543210") + "\",\"pin\":\"1122\",\"amount\":-5.00}]}\n"
                + "{\"member_id\":\"M7654321\",\"dob\":\"1936-07-01\",\"address\":{\"zip\":\"03601\"},\"cards\":[]}\n";
    }

    @Test
    void aJsonSourceExpandsCardsAndKeepsOneSurrogatePerMember() throws IOException {
        DeidJob.Result r = job(deid(profile("zenjson", ZEN_JSON), jsonSource(membersJsonl(), null), false), 2).run();
        List<String> out = lines(r.csv());
        assertEquals("member_ref,dob,address.zip,cards.card_number,cards.amount", out.get(0));
        assertEquals(4, out.size(), out.toString());
        String[] a = out.get(1).split(",", -1);
        String[] b = out.get(2).split(",", -1);
        String[] c = out.get(3).split(",", -1);
        assertEquals(a[0], b[0], "the member's two cards share the member surrogate");
        assertNotEquals(a[3], b[3], "two cards, two card surrogates");
        assertEquals(List.of("1985", "606", "25.00"), List.of(a[1], a[2], a[4]));
        assertEquals("'-5.00", b[4]);
        assertEquals(List.of("<=1936", "000", "", ""), List.of(c[1], c[2], c[3], c[4]), "no cards: one row, card fields empty");
        String text = String.join("\n", out);
        for (String s : SENSITIVE) {
            assertFalse(text.contains(s), s);
        }
        @SuppressWarnings("unchecked")
        List<String> dropped = (List<String>) r.manifest().get("dropped_by_default");
        assertTrue(dropped.contains("name.last"), String.valueOf(dropped));
        assertEquals(4, jdbc.queryForObject("SELECT COUNT(*) FROM deid_surrogate", Integer.class), "two members, two cards");
    }

    @Test
    void aJsonArrayFileGivesTheSameCsvAndADryRunListsPathsNotSeen() throws IOException {
        List<String> fromLines = lines(job(deid(profile("zenjson", ZEN_JSON), jsonSource(membersJsonl(), null), false), 10)
                .run().csv());
        Files.move(dir.resolve("out"), dir.resolve("out-lines"));
        String array = "[" + membersJsonl().strip().replace("\n", ",\n") + "]";
        Files.delete(dir.resolve("in/members.json"));
        List<String> fromArray = lines(job(deid(profile("zenjson", ZEN_JSON),
                jsonSource(array, ClientProperties.Deid.Json.Layout.ARRAY), false), 10).run().csv());
        assertEquals(fromLines, fromArray, "the second run reuses the surrogates, so the files match");

        PrintStream stdout = System.out;
        DeidJob.Result r;
        try {
            System.setOut(new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8));
            r = job(deid(profile("typo", ZEN_JSON.replace("address.zip", "adress.zip")),
                    jsonSource(array, ClientProperties.Deid.Json.Layout.ARRAY), true), 10).run();
        } finally {
            System.setOut(stdout);
        }
        assertEquals(List.of("adress.zip"), r.review().get("paths_not_seen"));
    }

    @Test
    void theExampleProfilesAreValid() throws IOException {
        List<String> columns = List.of("id", "member_id", "first_name", "last_name", "email", "phone", "dob", "zip",
                "card_number", "card_pin", "card_type", "amount", "status", "issued_date");
        for (String name : List.of("acme-fulfillment.yml", "zen-reporting.yml")) {
            Path p = Path.of("config-examples/deid-profiles", name);
            assertEquals(List.of(), com.hsm.client.deid.ProfileValidator.problems(
                    DeidJob.parseProfile(Files.readAllBytes(p)), columns), name);
        }
        Path json = Path.of("config-examples/deid-profiles/json-source-example.yml");
        com.hsm.client.deid.DeidProfile jp = DeidJob.parseProfile(Files.readAllBytes(json));
        List<String> paths = jp.fields().stream().map(com.hsm.client.deid.DeidProfile.Field::name).toList();
        assertEquals(List.of(), com.hsm.client.deid.ProfileValidator.problems(jp, paths), "json-source-example.yml");
        new com.hsm.client.deidjob.source.JsonRecordSource(new java.io.ByteArrayInputStream(new byte[0]),
                StandardCharsets.UTF_8, null, false, paths, paths).close();
    }
}
