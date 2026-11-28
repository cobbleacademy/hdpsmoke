package com.hsm.edekmigrate;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.util.Context;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The pieces that need no database: row hash, key ids, arguments, the HTTP stack. */
class UnitTest {

    private static EdekRow row(String fingerprint, String currentName) {
        return new EdekRow(UUID.fromString("00000000-0000-0000-0000-000000000001"), "payments-svc", "blob", "v1", null,
                null, null, null, "AES-256-GCM", "utf8", null, "current",
                OffsetDateTime.of(2026, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC), null, fingerprint, "n", currentName);
    }

    @Test
    void theSourceHashIsStableAndSeesEveryChange() {
        assertEquals(row("ab", "n").sourceHash(), row("ab", "n").sourceHash());
        assertNotEquals(row("ab", "n").sourceHash(), row("ac", "n").sourceHash());
        assertNotEquals(row("ab", "n").sourceHash(), row("ab", null).sourceHash(), "NULL differs from a value");
        assertNotEquals(row("ab", "").sourceHash(), row("ab", null).sourceHash(), "NULL differs from empty");
        // the same instant in another offset is the same row
        EdekRow utc = row("ab", "n");
        EdekRow other = new EdekRow(utc.edekId(), utc.appId(), utc.edekBlob(), utc.kekVersion(), null, null, null,
                null, utc.algorithm(), utc.encoding(), null, "current",
                utc.createdAt().withOffsetSameInstant(ZoneOffset.ofHours(5)), null, "ab", "n", "n");
        assertEquals(utc.sourceHash(), other.sourceHash());
    }

    @Test
    void aNullKekNameIsTheLegacyDefault() {
        assertEquals("hsm-master-kek", row("ab", null).resolvedKekName("hsm-master-kek"));
    }

    @Test
    void theKekVersionComesFromTheReturnedKeyId() {
        assertEquals("abc123", AzureKekVault.versionFromKeyId("https://v.vault.azure.net/keys/kek/abc123"));
        assertEquals("abc123", AzureKekVault.versionFromKeyId("https://v.vault.azure.net/keys/kek/abc123/"));
        assertThrows(IllegalStateException.class,
                () -> AzureKekVault.versionFromKeyId("https://v.vault.azure.net/keys/kek"));
    }

    @Test
    void zeroClearsTheDek() {
        byte[] dek = {1, 2, 3};
        Commands.zero(dek);
        assertArrayEquals(new byte[3], dek);
        Commands.zero(null);
    }

    @Test
    void anErrorIsReportedByItsTypeOnly() {
        assertEquals("IllegalStateException", Commands.errorType(new IllegalStateException("secret detail")));
    }

    @Test
    void errorTextKeepsTheReasonAndDropsSecrets() {
        assertEquals("localhost:5432/hsm", Errors.where("jdbc:postgresql://localhost:5432/hsm?sslmode=require"));
        assertEquals("[\"jdbc:postgresql://h/d\"]", Errors.where("\"jdbc:postgresql://h/d\""), "stray quotes show");
        assertEquals("No suitable driver found for jdbc:postgresql://h/d?user=x&password=<redacted>",
                Errors.redact("No suitable driver found for jdbc:postgresql://h/d?user=x&password=hunter2"));
        assertEquals("{\"value\":\"<redacted>\"} Bearer <redacted>",
                Errors.redact("{\"value\":\"c2VjcmV0\"} Bearer eyJ0eXAi.abc"));
        assertEquals("design=ok", Errors.redact("design=ok"));
        RuntimeException e = new IllegalStateException("wrap failed\nsecond line",
                new java.net.UnknownHostException("hsm-b.managedhsm.azure.net"));
        assertEquals("IllegalStateException: wrap failed (cause UnknownHostException: hsm-b.managedhsm.azure.net)",
                Errors.describe(e));
        java.sql.SQLException noDriver = new java.sql.SQLException("No suitable driver found for \"jdbc:x\"", "08001");
        assertTrue(Errors.connectHint("source", noDriver).startsWith("source.jdbc-url must look like"));
        assertTrue(Errors.connectHint("target", new java.sql.SQLException("x", "08001"))
                .contains("can't reach the server"));
        assertTrue(Errors.queryHint("target", new java.sql.SQLException("x", "42501")).contains("target.user"));
    }

    @Test
    void badArgumentsAreAUsageError(@TempDir Path dir) throws Exception {
        assertEquals(Commands.USAGE, run(new String[]{}));
        assertEquals(Commands.USAGE, run(new String[]{"copy", "--config", "x"}));
        assertEquals(Commands.USAGE, run(new String[]{"stage"}));
        assertEquals(Commands.USAGE, run(new String[]{"stage", "--config", "x", "--color"}));
        assertEquals(Commands.USAGE, run(new String[]{"stage", "--config", "x", "--kek"}));
        assertEquals(Commands.USAGE, run(new String[]{"stage", "--config", dir.resolve("none").toString()}));
        Path empty = Files.writeString(dir.resolve("empty.properties"), "");
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        assertEquals(Commands.USAGE, EdekMigrate.run(new String[]{"stage", "--config", empty.toString()}, Map.of(),
                new PrintStream(new ByteArrayOutputStream()), new PrintStream(err, true, StandardCharsets.UTF_8)));
        assertTrue(err.toString(StandardCharsets.UTF_8).contains("config: source.jdbc-url is required"));
    }

    @Test
    void theTemplateListsEveryKeyAndLoadsOnceFilledIn() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        assertEquals(Commands.OK, EdekMigrate.run(new String[]{"template"}, Map.of(),
                new PrintStream(out, true, StandardCharsets.UTF_8), new PrintStream(new ByteArrayOutputStream())));
        String template = out.toString(StandardCharsets.UTF_8);

        // Every key the loader reads appears in the template (set or commented out).
        for (String key : new String[]{"jdbc-url", "user", "password-env", "crypto-schema", "vault-url", "credential",
                "tenant-id", "client-id", "client-secret-env", "client-certificate-path"}) {
            assertTrue(template.contains("source." + key + "="), "source." + key);
            assertTrue(template.contains("target." + key + "="), "target." + key);
        }
        for (String key : new String[]{"source.legacy-kek-name=", "target.access-schema=", "target.staging-schema=",
                "kek-map.", "page-size=", "max-consecutive-failures=", "source.token-file=", "target.token-file=", "workload-identity"}) {
            assertTrue(template.contains(key), key);
        }

        // Unfilled, it's refused; with every <...> filled in and the secrets set, it loads.
        java.util.Properties raw = new java.util.Properties();
        raw.load(new java.io.StringReader(template));
        Map<String, String> env = Map.of("EDEK_SOURCE_DB_PASSWORD", "x", "EDEK_TARGET_DB_PASSWORD", "x",
                "EDEK_SOURCE_CLIENT_SECRET", "x", "EDEK_TARGET_CLIENT_SECRET", "x");
        assertThrows(MigrateConfig.ConfigException.class, () -> MigrateConfig.from(raw, env));
        java.util.Properties filled = new java.util.Properties();
        filled.load(new java.io.StringReader(template.replaceAll("<[^>]+>", "x")));
        MigrateConfig config = MigrateConfig.from(filled, env);
        assertEquals("client-secret", config.source().credential().mode());
        assertEquals("client-secret", config.target().credential().mode());
        assertEquals(Map.of("x", "x"), config.kekMap());
        assertEquals("edek_migrate", config.stagingSchema());
    }

    @Test
    void helpPrintsTheCommands() {
        for (String h : new String[]{"help", "--help", "-h"}) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            assertEquals(Commands.OK, EdekMigrate.run(new String[]{h}, Map.of(),
                    new PrintStream(out, true, StandardCharsets.UTF_8), new PrintStream(new ByteArrayOutputStream())));
            String text = out.toString(StandardCharsets.UTF_8);
            assertTrue(text.contains("template") && text.contains("promote") && text.contains("exit:"), text);
        }
    }

    @Test
    void argumentsParse() {
        EdekMigrate.Args a = EdekMigrate.Args.parse(new String[]{"reset", "--config", "m.properties", "--kek", "k1",
                "--app", "app1", "--yes", "--no-probe"});
        assertEquals("reset", a.command());
        assertEquals(new Scope("k1", "app1"), a.scope());
        assertTrue(a.yes());
        assertEquals(false, a.probe());
    }

    @Test
    void kekAndAppTakeListsAndRepeatsAddUp(@TempDir Path dir) throws Exception {
        EdekMigrate.Args a = EdekMigrate.Args.parse(new String[]{"stage", "--config", "m", "--kek", "k2, k1",
                "--kek", "k3", "--app", "a1,a2"});
        assertEquals(java.util.Set.of("k1", "k2", "k3"), a.scope().keks());
        assertEquals(java.util.Set.of("a1", "a2"), a.scope().apps());
        assertEquals("kek=k1,k2,k3 app=a1,a2", a.scope().toString());
        assertThrows(IllegalArgumentException.class,
                () -> EdekMigrate.Args.parse(new String[]{"stage", "--config", "m", "--kek", "k1,,k2"}));

        UUID one = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID two = UUID.fromString("00000000-0000-0000-0000-000000000002");
        Path ids = Files.writeString(dir.resolve("ids.txt"), "# trial sample\n" + two + "\n\n  " + one + "  \n" + one
                + "\n");
        EdekMigrate.Args b = EdekMigrate.Args.parse(new String[]{"promote", "--config", "m", "--ids", ids.toString(),
                "--kek", "k1"});
        assertEquals(java.util.Set.of(one, two), b.scope().ids());
        assertEquals("kek=k1 ids=2", b.scope().toString());

        Path bad = Files.writeString(dir.resolve("bad.txt"), one + "\nnot-an-id\n");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> EdekMigrate.Args.parse(new String[]{"stage", "--config", "m", "--ids", bad.toString()}));
        assertTrue(e.getMessage().contains("line 2"), e.getMessage());
        Path empty = Files.writeString(dir.resolve("empty.txt"), "# nothing\n");
        assertThrows(IllegalArgumentException.class,
                () -> EdekMigrate.Args.parse(new String[]{"stage", "--config", "m", "--ids", empty.toString()}));
        assertThrows(IllegalArgumentException.class, () -> EdekMigrate.Args.parse(new String[]{"stage", "--config",
                "m", "--ids", ids.toString(), "--ids", ids.toString()}), "one --ids file");
        assertThrows(IllegalArgumentException.class, () -> EdekMigrate.Args.parse(new String[]{"stage", "--config",
                "m", "--ids", dir.resolve("none.txt").toString()}));
    }

    @Test
    void theAzureSdkUsesTheJdkHttpClient() {
        HttpClient client = HttpClient.createDefault();
        assertEquals("com.azure.core.http.jdk.httpclient.JdkHttpClient", client.getClass().getName());
        Throwable t = assertThrows(Throwable.class, () -> client.sendSync(
                new HttpRequest(HttpMethod.GET, "http://127.0.0.1:9/"), Context.NONE).close());
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            assertTrue(!(c instanceof LinkageError), "the client failed to link, not to connect: " + c);
        }
        assertThrows(ClassNotFoundException.class, () -> Class.forName("reactor.netty.http.client.HttpClient"));
    }

    private static int run(String[] args) {
        return EdekMigrate.run(args, Map.of(), new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(new ByteArrayOutputStream()));
    }
}
