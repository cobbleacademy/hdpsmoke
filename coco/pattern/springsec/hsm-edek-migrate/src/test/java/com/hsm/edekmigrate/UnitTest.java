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
    void argumentsParse() {
        EdekMigrate.Args a = EdekMigrate.Args.parse(new String[]{"reset", "--config", "m.properties", "--kek", "k1",
                "--app", "app1", "--yes", "--no-probe"});
        assertEquals("reset", a.command());
        assertEquals(new Scope("k1", "app1"), a.scope());
        assertTrue(a.yes());
        assertEquals(false, a.probe());
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
