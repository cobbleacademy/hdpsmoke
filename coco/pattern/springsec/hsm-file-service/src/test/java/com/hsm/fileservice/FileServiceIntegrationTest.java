package com.hsm.fileservice;

import com.hsm.client.config.FipsBootstrap;
import com.hsm.client.crypto.DekManager;
import com.hsm.client.fileformat.EncryptedFileWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalManagementPort;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Boots the real service (embedded Tomcat, real HsmCryptoClient/SvcClient over HTTP)
 * against {@link FakeCoreService} and a LOCAL store, and checks every delivery mode
 * and error code a consumer can see. The abort test is the important one: a stream
 * that fails after bytes were sent must reach the client as a broken transfer, not a
 * short-but-normal response.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class FileServiceIntegrationTest {

    private static final String OWNER = "ingest-app";          // app that encrypted the files
    private static final String SERVICE_APP = "ui-file-service"; // this service's own app_id
    private static final int CHUNK = 4096;
    private static final int BUFFER_THRESHOLD = 20_000;           // stored bytes; the 64 KiB files stream

    private static final Path ROOT;
    private static final Path STORE;
    private static final FakeCoreService CORE;
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static final byte[] SMALL = random(10_000, 1);
    private static final byte[] BIG = random(64 * 1024, 2);
    private static UUID smallV2FileId;

    static {
        FipsBootstrap.register();
        try {
            ROOT = Files.createTempDirectory("hsm-file-service-test");
            STORE = Files.createDirectories(ROOT.resolve("store"));
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
            kpg.initialize(2048);
            KeyPair transport = kpg.generateKeyPair();
            Files.writeString(ROOT.resolve("private-key.pem"), "-----BEGIN PRIVATE KEY-----\n"
                    + Base64.getMimeEncoder().encodeToString(transport.getPrivate().getEncoded())
                    + "\n-----END PRIVATE KEY-----\n");
            CORE = new FakeCoreService("/api/sensec/hsm/v1", transport.getPublic());
            seedStore();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry r) {
        r.add("hsm.file-service.core.base-url", CORE::baseUrl);
        r.add("hsm.file-service.core.app-id", () -> SERVICE_APP);
        r.add("hsm.file-service.core.auth-mode", () -> "STATIC");
        r.add("hsm.file-service.core.static-token", () -> "test-token");
        r.add("hsm.file-service.core.private-key-pem-file", () -> ROOT.resolve("private-key.pem").toString());
        r.add("hsm.file-service.store.type", () -> "LOCAL");
        r.add("hsm.file-service.store.root", STORE::toString);
        r.add("hsm.file-service.access.allowed-path-prefixes", () -> "tenant-a,legacy");
        r.add("hsm.file-service.delivery.buffer-threshold-bytes", () -> BUFFER_THRESHOLD);
        r.add("management.server.port", () -> 0);
    }

    @LocalServerPort
    int port;

    @LocalManagementPort
    int managementPort;

    @AfterAll
    static void stopCore() {
        CORE.close();
    }

    // ---- fixtures ----

    private static byte[] random(int n, long seed) {
        byte[] b = new byte[n];
        new Random(seed).nextBytes(b);
        return b;
    }

    private static UUID newKey() {
        UUID id = UUID.randomUUID();
        CORE.deks.put(id, new FakeCoreService.Dek(DekManager.generateDek(), OWNER));
        return id;
    }

    private static EncryptedFileWriter.Result write(String path, byte[] plaintext, UUID edekId, EncryptedFileWriter.Options opts)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        EncryptedFileWriter.Result r = EncryptedFileWriter.write(new ByteArrayInputStream(plaintext), out, edekId,
                CORE.deks.get(edekId).raw(), OWNER, opts);
        Path target = STORE.resolve(path);
        Files.createDirectories(target.getParent());
        Files.write(target, out.toByteArray());
        return r;
    }

    /** Rewrites a stored v2 file with its frames rearranged by {@code edit}. */
    private static void tamper(String path, java.util.function.Consumer<List<byte[]>> edit) throws IOException {
        byte[] file = Files.readAllBytes(STORE.resolve(path));
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(file));
        byte[] header = in.readNBytes(41);
        List<byte[]> frames = new ArrayList<>();
        while (in.available() > 0) {
            int len = in.readInt();
            ByteArrayOutputStream f = new ByteArrayOutputStream();
            new java.io.DataOutputStream(f).writeInt(len);
            f.write(in.readNBytes(len));
            frames.add(f.toByteArray());
        }
        edit.accept(frames);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(header);
        frames.forEach(out::writeBytes);
        Files.write(STORE.resolve(path), out.toByteArray());
    }

    private static void seedStore() throws IOException {
        EncryptedFileWriter.Options v2 = EncryptedFileWriter.Options.v2(CHUNK, false);
        smallV2FileId = write("tenant-a/report.pdf", SMALL, newKey(), v2).fileId();
        write("legacy/scan.png", SMALL, newKey(), EncryptedFileWriter.Options.v1(CHUNK, false));
        write("tenant-a/big.bin", BIG, newKey(), EncryptedFileWriter.Options.v2(CHUNK, true));
        write("tenant-a/big-plain.bin", BIG, newKey(), v2);

        write("tenant-a/truncated.pdf", SMALL, newKey(), v2);
        tamper("tenant-a/truncated.pdf", frames -> frames.remove(frames.size() - 1));

        write("tenant-a/late-tamper.bin", BIG, newKey(), v2);
        tamper("tenant-a/late-tamper.bin", frames -> frames.set(10, frames.get(11)));

        write("tenant-a/early-tamper.bin", BIG, newKey(), v2);
        tamper("tenant-a/early-tamper.bin", frames -> frames.set(0, frames.get(1)));

        UUID unknown = newKey();
        write("tenant-a/revoked.pdf", SMALL, unknown, v2);
        CORE.deks.remove(unknown); // core now answers "not found / not authorized"

        UUID down = newKey();
        write("tenant-a/core-down.pdf", SMALL, down, v2);
        CORE.failHttp.add(down);

        write("other-tenant/secret.pdf", SMALL, newKey(), v2);
    }

    private HttpResponse<byte[]> get(String path, String... headers) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/files/" + path)).GET();
        for (int i = 0; i < headers.length; i += 2) {
            b.header(headers[i], headers[i + 1]);
        }
        return HTTP.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private static String body(HttpResponse<byte[]> r) {
        return new String(r.body(), java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void assertError(HttpResponse<byte[]> r, int status, String code) {
        assertEquals(status, r.statusCode(), body(r));
        assertTrue(body(r).contains("\"error_code\":\"" + code + "\""), body(r));
        assertTrue(r.headers().firstValue("X-Request-Id").isPresent(), "request id echoed on errors");
        assertEquals("no-store", r.headers().firstValue("Cache-Control").orElse(null));
    }

    // ---- happy paths ----

    @Test
    void smallV2File_isBuffered_withLengthIdsAndSafeHeaders() throws Exception {
        HttpResponse<byte[]> r = get("tenant-a/report.pdf", "X-End-User", "alice@example.com", "X-Request-Id", "req-123");
        assertEquals(200, r.statusCode());
        assertArrayEquals(SMALL, r.body());
        assertEquals("buffered", r.headers().firstValue("X-HSM-Delivery").orElseThrow());
        assertEquals(String.valueOf(SMALL.length), r.headers().firstValue("Content-Length").orElseThrow());
        assertEquals("application/pdf", r.headers().firstValue("Content-Type").orElseThrow());
        assertEquals("2", r.headers().firstValue("X-HSM-Format-Version").orElseThrow());
        assertEquals(smallV2FileId.toString(), r.headers().firstValue("X-HSM-File-Id").orElseThrow());
        assertEquals("no-store", r.headers().firstValue("Cache-Control").orElseThrow());
        assertEquals("nosniff", r.headers().firstValue("X-Content-Type-Options").orElseThrow());
        assertEquals("req-123", r.headers().firstValue("X-Request-Id").orElseThrow());
        assertTrue(r.headers().firstValue("Content-Disposition").orElseThrow().startsWith("inline"));
    }

    @Test
    void v1File_isServed() throws Exception {
        HttpResponse<byte[]> r = get("legacy/scan.png");
        assertEquals(200, r.statusCode());
        assertArrayEquals(SMALL, r.body());
        assertEquals("1", r.headers().firstValue("X-HSM-Format-Version").orElseThrow());
        assertFalse(r.headers().firstValue("X-HSM-File-Id").isPresent());
    }

    @Test
    void largeFile_isStreamed_withoutContentLength() throws Exception {
        for (String path : List.of("tenant-a/big.bin", "tenant-a/big-plain.bin")) {
            HttpResponse<byte[]> r = get(path);
            assertEquals(200, r.statusCode());
            assertArrayEquals(BIG, r.body(), path);
            assertEquals("streaming", r.headers().firstValue("X-HSM-Delivery").orElseThrow());
            assertFalse(r.headers().firstValue("Content-Length").isPresent());
        }
    }

    // ---- file-swap check ----

    @Test
    void expectedFileId_match_mismatch_v1_malformed() throws Exception {
        assertEquals(200, get("tenant-a/report.pdf", "X-Expected-File-Id", smallV2FileId.toString()).statusCode());
        assertError(get("tenant-a/report.pdf", "X-Expected-File-Id", UUID.randomUUID().toString()), 412, "FS-412-FILE-ID-MISMATCH");
        assertError(get("legacy/scan.png", "X-Expected-File-Id", UUID.randomUUID().toString()), 412, "FS-412-NO-FILE-ID");
        assertError(get("tenant-a/report.pdf", "X-Expected-File-Id", "not-a-uuid"), 400, "FS-400-BAD-FILE-ID");
    }

    // ---- integrity ----

    @Test
    void truncatedSmallFile_isCleanIntegrityError() throws Exception {
        assertError(get("tenant-a/truncated.pdf"), 422, "FS-422-INTEGRITY");
    }

    @Test
    void streamedFile_failingOnFirstChunk_isStillACleanError() throws Exception {
        assertError(get("tenant-a/early-tamper.bin"), 422, "FS-422-INTEGRITY");
    }

    @Test
    void streamedFile_failingAfterBytesWereSent_abortsTheConnection() {
        // Chunk 10 of 16 is out of order; by then ~40 KB have been sent, so the only
        // honest signal left is a broken transfer. The client must see an error.
        IOException e = assertThrows(IOException.class, () -> get("tenant-a/late-tamper.bin"));
        assertTrue(e.getMessage() == null || !e.getMessage().isBlank());
    }

    // ---- keys and core ----

    @Test
    void keyRefusedByCore_is502() throws Exception {
        assertError(get("tenant-a/revoked.pdf"), 502, "FS-502-KEY-UNAVAILABLE");
    }

    @Test
    void coreUnavailable_is503() throws Exception {
        assertError(get("tenant-a/core-down.pdf"), 503, "FS-503-CORE-UNAVAILABLE");
    }

    @Test
    void unwrapIsCachedPerEdekId() throws Exception {
        int before = CORE.unwrapCalls.get();
        get("tenant-a/big-plain.bin");
        get("tenant-a/big-plain.bin");
        get("tenant-a/big-plain.bin");
        assertTrue(CORE.unwrapCalls.get() - before <= 1, "at most one /dek/unwrap for repeated reads of one file");
    }

    // ---- paths ----

    @Test
    void pathOutsideAllowedPrefixes_isNotFound() throws Exception {
        assertError(get("other-tenant/secret.pdf"), 404, "FS-404-NOT-FOUND");
    }

    @Test
    void missingFile_isNotFound() throws Exception {
        assertError(get("tenant-a/nope.pdf"), 404, "FS-404-NOT-FOUND");
    }

    @Test
    void prefixMatchesOnSegmentBoundaryOnly() throws Exception {
        assertError(get("tenant-ab/report.pdf"), 404, "FS-404-NOT-FOUND");
    }

    @Test
    void encodedDotSegments_areRejected() throws Exception {
        HttpResponse<byte[]> r = get("tenant-a/%2E%2E/other-tenant/secret.pdf");
        assertTrue(r.statusCode() == 400 || r.statusCode() == 404, "status " + r.statusCode());
        assertFalse(body(r).contains("%PDF"));
        assertEquals(-1, indexOf(r.body(), SMALL), "must never serve the other tenant's file");
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        return java.util.Collections.indexOfSubList(toList(haystack), toList(needle));
    }

    private static List<Byte> toList(byte[] b) {
        List<Byte> l = new ArrayList<>(b.length);
        for (byte x : b) {
            l.add(x);
        }
        return l;
    }

    // ---- ops ----

    @Test
    void metricsAndProbes_areOnTheManagementPort() throws Exception {
        get("tenant-a/report.pdf");
        HttpResponse<String> prom = HTTP.send(HttpRequest.newBuilder(
                URI.create("http://localhost:" + managementPort + "/actuator/prometheus")).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, prom.statusCode());
        assertTrue(prom.body().contains("hsm_file_requests_total"), "request counter exported");
        HttpResponse<String> ready = HTTP.send(HttpRequest.newBuilder(
                URI.create("http://localhost:" + managementPort + "/actuator/health/readiness")).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, ready.statusCode());
        HttpResponse<String> notOnAppPort = HTTP.send(HttpRequest.newBuilder(
                URI.create("http://localhost:" + port + "/actuator/prometheus")).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(404, notOnAppPort.statusCode(), "metrics must not be reachable on the BFF-facing port");
    }
}
