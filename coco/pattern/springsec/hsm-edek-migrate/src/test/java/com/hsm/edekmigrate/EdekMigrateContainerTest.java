package com.hsm.edekmigrate;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The whole move against real Postgres: A and B are two databases with hsm-core's own
 * Flyway migrations, the HSMs are {@link FakeKekVault}s with real RSA-OAEP-256 keys.
 * Every test checks the end state the way hsm-core reads it: B's row, unwrapped with
 * B's KEK and version, gives the same DEK as A's row did. Skipped without Docker.
 */
@Testcontainers(disabledWithoutDocker = true)
class EdekMigrateContainerTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    private static final Path MIGRATIONS = Path.of("../hsm-core-service/src/main/resources/db/migration");
    private static final AtomicInteger DB_SEQ = new AtomicInteger();

    private static final String LEGACY = "hsm-master-kek";
    private static final String PAY = "payments-svc";
    private static final String REPORTING = "reporting-app";

    private Connection a;          // the test's own connection to A, for seeding and changes
    private Connection b;
    private String urlA;
    private String urlB;
    private final List<Connection> toolConnections = new ArrayList<>();
    private FakeKekVault vaultA;
    private FakeKekVault vaultB;
    private final Map<UUID, byte[]> deks = new LinkedHashMap<>();
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private Properties props;

    @BeforeEach
    void setUp() throws Exception {
        int n = DB_SEQ.incrementAndGet();
        urlA = database("a" + n);
        a = connect(urlA);
        urlB = database("b" + n);
        b = connect(urlB);
        vaultA = new FakeKekVault().kek(LEGACY, "a-v1", "a-v2").kek("kek-pii", "pii-v1");
        vaultB = new FakeKekVault().kek("b-kek", "b-v1").kek("b-kek-pii", "bpii-v1");
        props = new Properties();
        props.setProperty("source.jdbc-url", "jdbc:postgresql://unused/a");
        props.setProperty("source.user", "u");
        props.setProperty("source.password-env", "PW");
        props.setProperty("source.crypto-schema", "hsm_crypto");
        props.setProperty("source.vault-url", "https://a.vault.example");
        props.setProperty("source.legacy-kek-name", LEGACY);
        props.setProperty("source.credential", "default");
        props.setProperty("target.jdbc-url", "jdbc:postgresql://unused/b");
        props.setProperty("target.user", "u");
        props.setProperty("target.password-env", "PW");
        props.setProperty("target.crypto-schema", "hsm_crypto");
        props.setProperty("target.access-schema", "hsm_access");
        props.setProperty("target.vault-url", "https://b.vault.example");
        props.setProperty("target.credential", "default");
        props.setProperty("kek-map." + LEGACY, "b-kek");
        props.setProperty("kek-map.kek-pii", "b-kek-pii");
        props.setProperty("page-size", "2");   // several pages even for a few rows

        registerInB(PAY, true);
        registerInB(REPORTING, false);   // a replaced app: kept, inactive (design section 6)
        seedA();
    }

    @AfterEach
    void tearDown() throws SQLException {
        for (Connection c : toolConnections) {
            c.close();
        }
        if (a != null) {
            a.close();
        }
        if (b != null) {
            b.close();
        }
    }

    // ---- scenarios ----

    @Test
    void theWholeMoveGivesBTheSameDeksUnderBsKeks() throws Exception {
        Commands c = commands();
        assertEquals(Commands.OK, c.analyze(Scope.ALL, true), out());
        assertEquals(Commands.OK, c.stage(Scope.ALL), out());
        assertEquals(Map.of("staged", 7L), statuses());
        assertEquals(Commands.OK, c.wrap(Scope.ALL), out());
        assertEquals(Map.of("wrapped", 7L), statuses());
        assertEquals(Commands.OK, c.verify(Scope.ALL), out());
        assertEquals(Commands.OK, c.report(Scope.ALL), out());
        assertTrue(out().contains("ready to promote: yes"), out());
        assertEquals(Commands.OK, c.promote(Scope.ALL), out());
        assertEquals(Map.of("promoted", 7L), statuses());

        assertBMatchesA();
        assertNoKeyMaterialIn(out());
    }

    @Test
    void twoSourceKeksCanShareOneTargetKek() throws Exception {
        props.setProperty("kek-map.kek-pii", "b-kek");   // both A KEKs onto one B KEK
        Commands c = commands();
        assertEquals(Commands.OK, c.analyze(Scope.ALL, true), out());

        // Still moved and checked per source KEK.
        Scope pii = new Scope("kek-pii", null);
        assertEquals(Commands.OK, c.stage(pii), out());
        assertEquals(Commands.OK, c.wrap(pii), out());
        assertEquals(Commands.OK, c.verify(pii), out());
        assertEquals(Commands.OK, c.promote(pii), out());
        assertEquals(2, countB("SELECT COUNT(*) FROM hsm_crypto.edek_records WHERE kek_name = 'b-kek'"));

        assertEquals(Commands.OK, c.stage(Scope.ALL), out());
        assertEquals(Commands.OK, c.wrap(Scope.ALL), out());
        assertEquals(Commands.OK, c.verify(Scope.ALL), out());
        output.reset();
        assertEquals(Commands.OK, c.report(Scope.ALL), out());
        assertTrue(out().contains("kek kek-pii "), "report still groups by source KEK: " + out());
        assertTrue(out().contains("kek " + LEGACY + " "), out());
        assertEquals(Commands.OK, c.promote(Scope.ALL), out());
        assertEquals(Map.of("promoted", 7L), statuses());

        assertEquals(0, countB("SELECT COUNT(*) FROM hsm_crypto.edek_records WHERE kek_name <> 'b-kek'"));
        assertBMatchesA(Map.of(LEGACY, "b-kek", "kek-pii", "b-kek"));
        assertNoKeyMaterialIn(out());
    }

    @Test
    void oneKekAtATime() throws Exception {
        Commands c = commands();
        Scope pii = new Scope("kek-pii", null);
        assertEquals(Commands.OK, c.stage(pii), out());
        assertEquals(Commands.OK, c.wrap(pii), out());
        assertEquals(Commands.OK, c.verify(pii), out());
        assertEquals(Commands.OK, c.promote(pii), out());
        assertEquals(2, countB("SELECT COUNT(*) FROM hsm_crypto.edek_records"));
        assertEquals(2, countB("SELECT COUNT(*) FROM hsm_crypto.edek_records WHERE kek_name = 'b-kek-pii'"));

        Scope legacy = new Scope(LEGACY, null);
        assertEquals(Commands.OK, c.stage(legacy), out());
        assertEquals(Commands.OK, c.wrap(legacy), out());
        assertEquals(Commands.OK, c.verify(legacy), out());
        assertEquals(Commands.OK, c.promote(legacy), out());
        assertBMatchesA();
    }

    @Test
    void analyzeFindsAMissingKekMapAnUnregisteredOwnerAndClashes() throws Exception {
        props.remove("kek-map.kek-pii");
        try (Statement st = b.createStatement()) {
            st.execute("DELETE FROM hsm_access.app_registrations WHERE app_id = '" + PAY + "'");
        }
        UUID taken = deks.keySet().iterator().next();
        insert(b, row(taken, "someone-else", "b-kek", "b-v1", vaultB.wrapWith(new byte[32], "b-kek", "b-v1"),
                null, "current", null));
        insert(b, row(UUID.randomUUID(), "someone-else", "b-kek", "b-v1", vaultB.wrapWith(new byte[32], "b-kek", "b-v1"),
                "customers.ssn", "current", "customers.ssn"));
        b.commit();

        assertEquals(Commands.PROBLEMS, commands().analyze(Scope.ALL, true));
        String o = out();
        assertTrue(o.contains("source KEK 'kek-pii' has no kek-map entry"), o);
        assertTrue(o.contains("owner app_id '" + PAY + "' has no app_registrations row"), o);
        assertTrue(o.contains("1 edek_id(s) already in the target"), o);
        assertTrue(o.contains("1 current_dek_name(s) already held"), o);
        assertEquals(Commands.PROBLEMS, commands().stage(Scope.ALL), "stage refuses an unmapped KEK");
    }

    @Test
    void analyzeProbesBothVaults() throws Exception {
        vaultA.failAll = true;
        assertEquals(Commands.PROBLEMS, commands().analyze(Scope.ALL, true));
        assertTrue(out().contains("can't unwrap with source KEK"), out());
        assertTrue(out().contains("IllegalStateException: injected failure"), "the vault's reason is shown: " + out());
        assertEquals(Commands.OK, commands().analyze(Scope.ALL, false), "--no-probe skips the HSMs");
    }

    @Test
    void aFailedRowBlocksPromoteUntilItSucceeds() throws Exception {
        Commands c = commands();
        c.stage(Scope.ALL);
        String bad = sourceBlobOf(deks.keySet().iterator().next());
        vaultA.failingEdeks.add(bad);

        assertEquals(Commands.PROBLEMS, c.wrap(Scope.ALL), out());
        assertEquals(Map.of("wrapped", 6L, "failed", 1L), statuses());
        assertTrue(out().contains("error=IllegalStateException"), out());
        // The vault's reason is printed once per error type; the staging row keeps only the type.
        assertTrue(out().contains("wrap reason for IllegalStateException: IllegalStateException: injected failure"),
                out());
        assertEquals(1, out().split("injected failure", -1).length - 1, out());
        assertEquals(1, countB("SELECT COUNT(*) FROM edek_migrate.edek_records WHERE error_type = "
                + "'IllegalStateException'"));
        c.verify(Scope.ALL);
        assertEquals(Commands.PROBLEMS, c.promote(Scope.ALL), out());
        assertEquals(0, countB("SELECT COUNT(*) FROM hsm_crypto.edek_records"), "nothing promoted");

        vaultA.failingEdeks.clear();
        assertEquals(Commands.OK, c.wrap(Scope.ALL), out());   // picks up the failed row only
        assertEquals(Commands.OK, c.verify(Scope.ALL), out());
        assertEquals(Commands.OK, c.promote(Scope.ALL), out());
        assertBMatchesA();
    }

    @Test
    void verifyCatchesABlobThatHoldsAnotherDek() throws Exception {
        Commands c = commands();
        c.stage(Scope.ALL);
        c.wrap(Scope.ALL);
        UUID victim = deks.keySet().iterator().next();
        byte[] other = new byte[32];
        new SecureRandom().nextBytes(other);
        try (PreparedStatement ps = b.prepareStatement("UPDATE edek_migrate.edek_records SET edek_blob = ?, "
                + "kek_version = 'b-v1' WHERE edek_id = ?")) {
            ps.setString(1, Base64.getEncoder().encodeToString(vaultB.wrapWith(other, "b-kek", "b-v1")));
            ps.setObject(2, victim);
            ps.executeUpdate();
        }
        b.commit();

        assertEquals(Commands.PROBLEMS, c.verify(Scope.ALL), out());
        assertTrue(out().contains("edek_id=" + victim + " source_kek=" + LEGACY + " error=DekMismatch"), out());
        assertEquals(Commands.PROBLEMS, c.promote(Scope.ALL), out());
    }

    @Test
    void theCutoverPassUpdatesChangedRowsAndAddsNewOnes() throws Exception {
        Commands c = commands();
        runAll(c);

        // In A, after the first promote: named-DEK rotation moved customers.ssn to a new row.
        UUID oldCurrent = idWithCurrentName("customers.ssn");
        try (PreparedStatement ps = a.prepareStatement("UPDATE hsm_crypto.edek_records SET rotation_status = "
                + "'rotated', rotated_at = now(), current_dek_name = NULL WHERE edek_id = ?")) {
            ps.setObject(1, oldCurrent);
            ps.executeUpdate();
        }
        UUID fresh = UUID.randomUUID();
        addToA(fresh, PAY, LEGACY, "a-v2", "customers.ssn", "current", "customers.ssn");
        a.commit();

        assertEquals(Commands.OK, c.stage(Scope.ALL), out());
        assertTrue(out().contains("added=1 changed=1 unchanged=6"), out());
        assertEquals(Commands.OK, c.wrap(Scope.ALL), out());
        assertEquals(Commands.OK, c.verify(Scope.ALL), out());
        assertEquals(Commands.OK, c.promote(Scope.ALL), out());
        assertTrue(out().contains("updated=1 inserted=1"), out());
        assertBMatchesA();
        assertEquals(fresh, idWithCurrentNameInB("customers.ssn"), "the name moved to the new row in B too");
    }

    @Test
    void aRowGoneFromABlocksPromote() throws Exception {
        Commands c = commands();
        c.stage(Scope.ALL);
        try (PreparedStatement ps = a.prepareStatement("DELETE FROM hsm_crypto.edek_records WHERE edek_id = ?")) {
            ps.setObject(1, deks.keySet().iterator().next());
            ps.executeUpdate();
        }
        a.commit();
        c.stage(Scope.ALL);
        assertTrue(out().contains("removed=1"), out());
        c.wrap(Scope.ALL);
        c.verify(Scope.ALL);
        assertEquals(Commands.PROBLEMS, c.promote(Scope.ALL), out());
        assertTrue(out().contains("rows not verified: {removed=1}"), out());
    }

    @Test
    void manyFailuresInARowStopTheRun() throws Exception {
        props.setProperty("max-consecutive-failures", "3");
        Commands c = commands();
        c.stage(Scope.ALL);
        vaultA.failAll = true;
        assertEquals(Commands.STOPPED, c.wrap(Scope.ALL), out());
        assertEquals(Map.of("failed", 3L, "staged", 4L), statuses());
        assertTrue(out().contains("wrap stopped: 3 failures in a row"), out());
    }

    @Test
    void resetNeedsYes() throws Exception {
        Commands c = commands();
        c.stage(Scope.ALL);
        assertEquals(Commands.USAGE, c.reset(Scope.ALL, false));
        assertEquals(7, countB("SELECT COUNT(*) FROM edek_migrate.edek_records"));
        assertEquals(Commands.OK, c.reset(new Scope(null, REPORTING), true));
        assertEquals(5, countB("SELECT COUNT(*) FROM edek_migrate.edek_records"));
    }

    @Test
    void aRowAlreadyInBUnderAnotherIdBlocksPromote() throws Exception {
        Commands c = commands();
        c.stage(Scope.ALL);
        c.wrap(Scope.ALL);
        c.verify(Scope.ALL);
        UUID taken = deks.keySet().iterator().next();
        insert(b, row(taken, PAY, "b-kek", "b-v1", vaultB.wrapWith(new byte[32], "b-kek", "b-v1"),
                null, "current", null));
        b.commit();
        assertEquals(Commands.PROBLEMS, c.promote(Scope.ALL), out());
        assertTrue(out().contains("1 verified row(s) whose edek_id is already in the target"), out());
    }

    // ---- checks ----

    /** Every A row is in B with the same columns, B's KEK, no undo buffer, and the same DEK. */
    // ---- several KEKs, and a chosen sample of edek_ids ----

    @Test
    void severalKeksInOneScope() throws Exception {
        Commands c = commands();
        Scope both = new Scope(java.util.Set.of(LEGACY, "kek-pii"), java.util.Set.of(), java.util.Set.of());
        assertEquals(Commands.OK, c.analyze(both, true), out());
        assertEquals(Commands.OK, c.stage(both), out());
        assertEquals(Map.of("staged", 7L), statuses());
        assertEquals(Commands.OK, c.stage(new Scope("kek-pii", null)), out());
        assertEquals(Map.of("staged", 7L), statuses(), "a narrower stage marks nothing outside it removed");
    }

    @Test
    void aTrialPromoteOfASampleThenTheRest() throws Exception {
        Commands c = commands();
        List<UUID> all = new ArrayList<>(deks.keySet());
        Scope sample = new Scope(java.util.Set.of(), java.util.Set.of(), java.util.Set.of(all.get(0), all.get(3)));

        assertEquals(Commands.OK, c.stage(sample), out());
        assertEquals(Map.of("staged", 2L), statuses(), "stage --ids stages only the sample");
        assertEquals(Commands.OK, c.stage(Scope.ALL), out());
        assertEquals(Commands.OK, c.wrap(sample), out());
        assertEquals(Commands.OK, c.verify(sample), out());
        assertEquals(Map.of("verified", 2L, "staged", 5L), statuses());

        assertEquals(Commands.PROBLEMS, c.promote(Scope.ALL), "the unverified rest blocks a full promote");
        assertEquals(Commands.OK, c.report(sample), out());
        assertTrue(out().contains("id edek_id=" + all.get(0) + " source_kek="), out());
        assertTrue(out().contains("ready to promote: yes"), out());
        assertEquals(Commands.OK, c.promote(sample), out());
        assertTrue(out().contains("updated=0 inserted=2"), out());
        assertTrue(out().contains("promote the rest before it takes traffic"), out());
        assertEquals(2, countB("SELECT COUNT(*) FROM hsm_crypto.edek_records"));

        output.reset();
        runAll(c);
        assertTrue(out().contains("updated=0 inserted=5"), "the sample isn't promoted twice: " + out());
        assertBMatchesA();
    }

    @Test
    void idsNotInTheSourceOrNotStagedAreReported() throws Exception {
        Commands c = commands();
        UUID known = deks.keySet().iterator().next();
        UUID unknown = UUID.randomUUID();
        Scope ids = new Scope(java.util.Set.of(), java.util.Set.of(), java.util.Set.of(known, unknown));
        assertEquals(Commands.PROBLEMS, c.analyze(ids, false), out());
        assertTrue(out().contains("--ids edek_id=" + unknown + " is not in the source"), out());
        assertTrue(out().contains("1 of the 2 --ids aren't in the source"), out());

        c.stage(new Scope(java.util.Set.of(), java.util.Set.of(), java.util.Set.of(known)));
        assertEquals(Commands.OK, c.report(ids), out());
        assertTrue(out().contains("id edek_id=" + unknown + " not staged"), out());
        assertTrue(out().contains("id edek_id=" + known + " source_kek="), out());
        assertTrue(out().contains("ready to promote: no"), out());
    }

    // ---- what the tool prints when a database call fails ----

    @Test
    void aWrongPasswordNamesTheSideHostAndUser(@TempDir Path dir) throws Exception {
        String err = runTool(dir, Commands.STOPPED, Map.of("PW", "not-the-password"));
        assertTrue(err.contains("can't connect to the source database " + hostPortDb(urlA) + " as "
                + POSTGRES.getUsername() + ": password authentication failed"), err);
        assertTrue(err.contains("(SQLState 28P01)"), err);
        assertTrue(err.contains("check: the server was reached and rejected the user or password"), err);
        assertFalse(err.contains("not-the-password"), err);
    }

    @Test
    void anUnreachableServerNamesTheSide(@TempDir Path dir) throws Exception {
        toolProps(urlA, urlB);
        props.setProperty("target.jdbc-url", "jdbc:postgresql://localhost:1/b");
        String err = runTool(dir, Commands.STOPPED, null);
        assertTrue(err.contains("can't connect to the target database localhost:1/b as "), err);
        assertTrue(err.contains("(SQLState 08001)"), err);
        assertTrue(err.contains("check: this machine can't reach the server"), err);
    }

    @Test
    void quotedValuesWorkAsInYaml(@TempDir Path dir) throws Exception {
        toolProps("\"" + urlA + "\"", "'" + urlB + "'");
        props.setProperty("source.user", "\"" + POSTGRES.getUsername() + "\"");
        String err = runTool(dir, Commands.OK, null);
        assertEquals("", err);
        assertTrue(out().contains("analyze ok"), out());
    }

    @Test
    void aMissingGrantNamesTheStepAndTheObject(@TempDir Path dir) throws Exception {
        String role = "nogrant" + DB_SEQ.incrementAndGet();
        try (Connection admin = DriverManager.getConnection(urlB, POSTGRES.getUsername(), POSTGRES.getPassword());
             Statement st = admin.createStatement()) {
            st.execute("CREATE ROLE " + role + " LOGIN PASSWORD 'pw'");
        }
        toolProps(urlA, urlB);
        props.setProperty("target.user", role);
        props.setProperty("target.password-env", "ROLE_PW");
        String err = runTool(dir, Commands.STOPPED, Map.of("PW", POSTGRES.getPassword(), "ROLE_PW", "pw"));
        assertTrue(err.contains("analyze stopped: target database, checking owners in hsm_access.app_registrations: "
                + "permission denied for schema hsm_access (SQLState 42501)"), err);
        assertTrue(err.contains("check: target.user is missing a grant"), err);
    }

    @Test
    void aWrongSchemaNamesTheTable(@TempDir Path dir) throws Exception {
        toolProps(urlA, urlB);
        props.setProperty("source.crypto-schema", "no_such_schema");
        String err = runTool(dir, Commands.STOPPED, null);
        assertTrue(err.contains("analyze stopped: source database, counting rows in no_such_schema.edek_records: "
                + "relation \"no_such_schema.edek_records\" does not exist (SQLState 42P01)"), err);
        assertTrue(err.contains("check: a table or schema isn't there: check source.crypto-schema"), err);
        assertFalse(err.contains("Detail"), err);
    }

    /** The settings for EdekMigrate.run against the test databases; vaults unused (analyze --no-probe). */
    private void toolProps(String sourceUrl, String targetUrl) {
        props.setProperty("source.jdbc-url", sourceUrl);
        props.setProperty("target.jdbc-url", targetUrl);
        props.setProperty("source.user", POSTGRES.getUsername());
        props.setProperty("target.user", POSTGRES.getUsername());
    }

    /** Runs {@code analyze --no-probe} through main's path; returns stderr. env null: the right password. */
    private String runTool(Path dir, int expected, Map<String, String> env) throws Exception {
        if (props.getProperty("source.user").equals("u")) {
            toolProps(urlA, urlB);
        }
        Path file = dir.resolve("m.properties");
        try (var w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
            props.store(w, null);
        }
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int code = EdekMigrate.run(new String[]{"analyze", "--config", file.toString(), "--no-probe"},
                env == null ? Map.of("PW", POSTGRES.getPassword()) : env,
                new PrintStream(output, true, StandardCharsets.UTF_8), new PrintStream(err, true, StandardCharsets.UTF_8));
        String text = err.toString(StandardCharsets.UTF_8);
        assertEquals(expected, code, text + out());
        return text;
    }

    private static String hostPortDb(String url) {
        String rest = url.substring("jdbc:postgresql://".length());
        int q = rest.indexOf('?');
        return q < 0 ? rest : rest.substring(0, q);
    }

    private void assertBMatchesA() throws Exception {
        assertBMatchesA(Map.of(LEGACY, "b-kek", "kek-pii", "b-kek-pii"));
    }

    /** {@code bKekFor}: the B KEK each source KEK's rows must end up on. */
    private void assertBMatchesA(Map<String, String> bKekFor) throws Exception {
        Map<UUID, EdekRow> inA = rows(a);
        Map<UUID, EdekRow> inB = rows(b);
        assertEquals(inA.keySet(), inB.keySet());
        for (EdekRow ra : inA.values()) {
            EdekRow rb = inB.get(ra.edekId());
            assertEquals(ra.appId(), rb.appId(), "app_id is the AAD: never changed");
            assertEquals(ra.fingerprint(), rb.fingerprint());
            assertEquals(ra.dekName(), rb.dekName());
            assertEquals(ra.currentDekName(), rb.currentDekName());
            assertEquals(ra.rotationStatus(), rb.rotationStatus());
            assertEquals(ra.algorithm(), rb.algorithm());
            assertEquals(ra.encoding(), rb.encoding());
            assertEquals(ra.dataClassification(), rb.dataClassification());
            assertEquals(instant(ra.createdAt()), instant(rb.createdAt()));
            assertEquals(instant(ra.rotatedAt()), instant(rb.rotatedAt()));
            assertEquals(bKekFor.get(ra.resolvedKekName(LEGACY)), rb.kekName());
            assertNull(rb.previousKekName());
            assertNull(rb.previousKekVersion());
            assertNull(rb.previousEdekBlob());
            byte[] dek = vaultB.unwrap(Base64.getDecoder().decode(rb.edekBlob()), rb.kekName(), rb.kekVersion());
            assertArrayEquals(deks.get(ra.edekId()), dek, "B gives the same DEK as A for " + ra.edekId());
        }
    }

    private void assertNoKeyMaterialIn(String text) {
        for (byte[] dek : deks.values()) {
            assertFalse(text.contains(Base64.getEncoder().encodeToString(dek)));
            assertFalse(text.contains(HexFormat.of().formatHex(dek)));
        }
    }

    // ---- fixtures ----

    /** A: 7 rows on 3 KEK versions; legacy NULL kek_name rows, a rotated row, an earlier rekey's undo buffer. */
    private void seedA() throws Exception {
        addToA(UUID.randomUUID(), PAY, null, "a-v1", "customers.ssn", "current", "customers.ssn");
        addToA(UUID.randomUUID(), PAY, null, "a-v1", "customers.ssn", "rotated", null);
        addToA(UUID.randomUUID(), PAY, null, "a-v1", null, "current", null);
        addToA(UUID.randomUUID(), REPORTING, LEGACY, "a-v2", "reports.q1", "current", "reports.q1");
        addToA(UUID.randomUUID(), REPORTING, LEGACY, "a-v2", null, "current", null);
        addToA(UUID.randomUUID(), PAY, "kek-pii", "pii-v1", "pii.dob", "current", "pii.dob");
        UUID rekeyed = UUID.randomUUID();
        addToA(rekeyed, PAY, "kek-pii", "pii-v1", null, "current", null);
        try (PreparedStatement ps = a.prepareStatement("UPDATE hsm_crypto.edek_records SET previous_kek_name = ?, "
                + "previous_kek_version = 'a-v1', previous_edek_blob = 'AAAA' WHERE edek_id = ?")) {
            ps.setString(1, LEGACY);
            ps.setObject(2, rekeyed);
            ps.executeUpdate();
        }
        a.commit();
    }

    private void addToA(UUID id, String app, String kekName, String version, String dekName, String status,
                        String currentName) throws SQLException {
        byte[] dek = new byte[32];
        new SecureRandom().nextBytes(dek);
        deks.put(id, dek);
        String wrapKek = kekName == null ? LEGACY : kekName;
        EdekRow r = row(id, app, kekName, version, vaultA.wrapWith(dek, wrapKek, version), dekName, status, currentName);
        insert(a, r);
    }

    private static EdekRow row(UUID id, String app, String kekName, String version, byte[] edek, String dekName,
                               String status, String currentName) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC).withNano(0);
        byte[] fp = new byte[8];
        new SecureRandom().nextBytes(fp);
        return new EdekRow(id, app, Base64.getEncoder().encodeToString(edek), version, kekName, null, null, null,
                "AES-256-GCM", "utf8", status.equals("rotated") ? "pii" : null, status, now,
                status.equals("rotated") ? now : null, HexFormat.of().formatHex(fp), dekName, currentName);
    }

    private static void insert(Connection db, EdekRow r) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement("INSERT INTO hsm_crypto.edek_records (" + EdekRow.COLUMNS
                + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            Object[] v = {r.edekId(), r.appId(), r.edekBlob(), r.kekVersion(), r.kekName(), r.previousKekName(),
                    r.previousKekVersion(), r.previousEdekBlob(), r.algorithm(), r.encoding(), r.dataClassification(),
                    r.rotationStatus(), r.createdAt(), r.rotatedAt(), r.fingerprint(), r.dekName(), r.currentDekName()};
            for (int i = 0; i < v.length; i++) {
                ps.setObject(i + 1, v[i]);
            }
            ps.executeUpdate();
        }
    }

    private void registerInB(String app, boolean active) throws SQLException {
        try (PreparedStatement ps = b.prepareStatement("INSERT INTO hsm_access.app_registrations (app_id, "
                + "allowed_scopes, description, active, created_at, updated_at) VALUES (?, 'encrypt,decrypt', '', ?, "
                + "now(), now())")) {
            ps.setString(1, app);
            ps.setBoolean(2, active);
            ps.executeUpdate();
        }
        b.commit();
    }

    private Commands commands() throws SQLException {
        MigrateConfig config = MigrateConfig.from(props, Map.of("PW", "x"));
        // The tool's own source connection, as in production: Commands makes it read-only.
        Connection source = DriverManager.getConnection(urlA, POSTGRES.getUsername(), POSTGRES.getPassword());
        toolConnections.add(source);
        return new Commands(config, source, b, vaultA, vaultB, new PrintStream(output, true, StandardCharsets.UTF_8));
    }

    private void runAll(Commands c) throws SQLException {
        assertEquals(Commands.OK, c.stage(Scope.ALL), out());
        assertEquals(Commands.OK, c.wrap(Scope.ALL), out());
        assertEquals(Commands.OK, c.verify(Scope.ALL), out());
        assertEquals(Commands.OK, c.promote(Scope.ALL), out());
    }

    private String out() {
        return output.toString(StandardCharsets.UTF_8);
    }

    private Map<String, Long> statuses() throws SQLException {
        Map<String, Long> out = new HashMap<>();
        try (Statement st = b.createStatement(); ResultSet rs = st.executeQuery(
                "SELECT migrate_status, COUNT(*) FROM edek_migrate.edek_records GROUP BY 1")) {
            while (rs.next()) {
                out.put(rs.getString(1), rs.getLong(2));
            }
        }
        b.rollback();
        return out;
    }

    private long countB(String sql) throws SQLException {
        try (Statement st = b.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            long n = rs.getLong(1);
            b.rollback();
            return n;
        }
    }

    private String sourceBlobOf(UUID id) throws SQLException {
        try (PreparedStatement ps = a.prepareStatement("SELECT edek_blob FROM hsm_crypto.edek_records WHERE edek_id = ?")) {
            ps.setObject(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }

    private UUID idWithCurrentName(String name) throws SQLException {
        return idWithCurrentName(a, name);
    }

    private UUID idWithCurrentNameInB(String name) throws SQLException {
        UUID id = idWithCurrentName(b, name);
        b.rollback();
        return id;
    }

    private static UUID idWithCurrentName(Connection db, String name) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement(
                "SELECT edek_id FROM hsm_crypto.edek_records WHERE current_dek_name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getObject(1, UUID.class) : null;
            }
        }
    }

    private static Map<UUID, EdekRow> rows(Connection db) throws SQLException {
        Map<UUID, EdekRow> out = new LinkedHashMap<>();
        try (Statement st = db.createStatement(); ResultSet rs = st.executeQuery(
                "SELECT " + EdekRow.COLUMNS + " FROM hsm_crypto.edek_records ORDER BY edek_id")) {
            while (rs.next()) {
                EdekRow r = EdekRow.read(rs);
                out.put(r.edekId(), r);
            }
        }
        if (!db.getAutoCommit()) {
            db.rollback();
        }
        return out;
    }

    private static Object instant(OffsetDateTime t) {
        return t == null ? null : t.toInstant();
    }

    private static Connection connect(String url) throws SQLException {
        Connection c = DriverManager.getConnection(url, POSTGRES.getUsername(), POSTGRES.getPassword());
        c.setAutoCommit(false);
        return c;
    }

    /** A fresh database with hsm-core-service's own migrations; its JDBC URL. */
    private static String database(String name) throws SQLException {
        try (Connection admin = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(),
                POSTGRES.getPassword()); Statement st = admin.createStatement()) {
            st.execute("CREATE DATABASE " + name);
        }
        String url = POSTGRES.getJdbcUrl().replaceAll("/[^/?]+(\\?|$)", "/" + name + "$1");
        Flyway.configure()
                .dataSource(url, POSTGRES.getUsername(), POSTGRES.getPassword())
                .locations("filesystem:" + MIGRATIONS.toAbsolutePath())
                .schemas("hsm_access")
                // No search_path: every migration names its schema (V14's DROP INDEX
                // didn't before), so this also checks they run without one.
                .placeholders(Map.of("crypto_schema", "hsm_crypto", "access_schema", "hsm_access"))
                .load()
                .migrate();
        return url;
    }
}
