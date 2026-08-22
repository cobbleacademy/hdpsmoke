package com.hsm.client.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hsm.client.config.FipsBootstrap;
import com.hsm.client.crypto.DekManager;
import com.hsm.client.crypto.TransportWrapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Cross-service, end-to-end regression guard for the interoperability
 * guarantee established this session: a file hsm-bulk-service's /dek/issue
 * + FileBulkJob's local encrypt produces must be directly decryptable via
 * hsm-core-service's own, unchanged /decrypt -- and the reverse, a value
 * hsm-core-service's own /encrypt produces must be resolvable via
 * hsm-bulk-service's /dek/unwrap for local decrypt. Fails loudly (a plain
 * assertion failure) if either direction ever breaks -- e.g. if
 * FileBulkJob.reconstructCoreServiceToken() or its base64 plaintext-safety
 * encoding regresses.
 *
 * <p>Spawns the REAL, already-built hsm-core-service.jar and
 * hsm-bulk-service.jar as OS subprocesses -- not Testcontainers/Docker
 * (unlike CheckpointStoreTest), since Docker isn't available in every
 * environment this needs to run in, and building two Spring Boot images
 * just for this test would be slow even where it is. Both share one
 * isolated, per-run temp H2 file (AUTO_SERVER=TRUE, matching the manual
 * live verification this class automates) and dynamically-chosen free
 * ports, so this never collides with a developer's own long-running demo
 * instance.
 *
 * <p>Skips gracefully (Assumptions, same spirit as CheckpointStoreTest's
 * {@code @EnabledIfDockerAvailable}) if the sibling modules' jars aren't
 * built -- hsm-bulk-client doesn't Maven-depend on either, so a bare
 * {@code mvn test} here alone can't guarantee they exist. Runs for real,
 * and actually guards against regressions, whenever the full reactor has
 * been built ({@code mvn -am package} first, or a full CI build).
 *
 * <p>Mirrors FileBulkJob.encryptOneFile's literal per-chunk logic directly
 * rather than reflecting into that private method -- same reasoning as
 * PaginationSyntaxTest's own javadoc: it isn't independently invokable
 * without a full SvcClient/DEK-issuance stub.
 * FileBulkJob.reconstructCoreServiceToken() itself IS called directly --
 * it's public, and it's the actual thing this class exists to guard.
 */
class CoreBulkFileInteropTest {

    private static final Duration START_TIMEOUT = Duration.ofSeconds(60);
    private static final String APP_ID = "payments-svc";
    private static final String DEMO_TOKEN = "demo-token-payments-svc";
    private static final String API_V1_PREFIX = "/api/sensec/hsm/v1";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @TempDir
    static Path tempDir;

    private static Process coreProcess;
    private static Process bulkProcess;
    private static int corePort;
    private static int bulkPort;
    private static PrivateKey testPrivateKey;

    @BeforeAll
    static void startServices() throws Exception {
        Path coreJar = Path.of("..", "hsm-core-service", "target", "hsm-core-service.jar").toAbsolutePath().normalize();
        Path bulkJar = Path.of("..", "hsm-bulk-service", "target", "hsm-bulk-service.jar").toAbsolutePath().normalize();
        Assumptions.assumeTrue(Files.exists(coreJar) && Files.exists(bulkJar),
                "Sibling service jars not built -- run `mvn -am package` from java/ first to exercise this test. "
                        + "Skipping (not failing): " + coreJar + " / " + bulkJar);

        FipsBootstrap.register();

        String dbUrl = "jdbc:h2:file:" + tempDir.resolve("interop_test_h2")
                + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;AUTO_SERVER=TRUE";

        corePort = findFreePort();
        bulkPort = findFreePort();

        coreProcess = startJar(coreJar, tempDir.resolve("core-service.log"), Map.of(
                "DEMO_MODE", "true",
                "SERVER_PORT", String.valueOf(corePort),
                "DEMO_DATABASE_URL", dbUrl
        ));
        waitForPort(corePort, START_TIMEOUT);

        bulkProcess = startJar(bulkJar, tempDir.resolve("bulk-service.log"), Map.of(
                "DEMO_MODE", "true",
                "SERVER_PORT", String.valueOf(bulkPort),
                "DATABASE_URL", dbUrl,
                "DATABASE_USERNAME", "sa",
                "DATABASE_PASSWORD", ""
        ));
        waitForPort(bulkPort, START_TIMEOUT);

        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        KeyPair keyPair = kpg.generateKeyPair();
        testPrivateKey = keyPair.getPrivate();
        String publicKeyPem = "-----BEGIN PUBLIC KEY-----\n"
                + Base64.getEncoder().encodeToString(keyPair.getPublic().getEncoded())
                + "\n-----END PUBLIC KEY-----\n";

        // payments-svc's row already exists by now -- DemoSeedInitializer's
        // @PostConstruct runs during hsm-core-service's context refresh,
        // which completes before Tomcat (and therefore waitForPort above)
        // ever opens the port. Test-side provisioning only -- neither
        // service's own code changes for this test to exist.
        try (Connection conn = DriverManager.getConnection(dbUrl, "sa", "");
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE app_registrations SET allowed_scopes = ?, public_key_pem = ? WHERE app_id = ?")) {
            ps.setString(1, "encrypt,decrypt,dek_issue,dek_unwrap");
            ps.setString(2, publicKeyPem);
            ps.setString(3, APP_ID);
            int updated = ps.executeUpdate();
            if (updated != 1) {
                throw new IllegalStateException(
                        "expected to provision exactly one app_registrations row for " + APP_ID + ", updated " + updated);
            }
        }
    }

    @AfterAll
    static void stopServices() {
        if (bulkProcess != null) {
            bulkProcess.destroyForcibly();
        }
        if (coreProcess != null) {
            coreProcess.destroyForcibly();
        }
    }

    @Test
    void bulkEncryptedChunks_decryptDirectlyViaCoreService() throws Exception {
        // Deliberately non-UTF-8-safe random bytes, not text -- exactly the
        // input that would silently corrupt if someone ever reverts
        // encryptOneFile's base64 plaintext-safety encoding (see
        // FileBulkJob's class javadoc). A text-safe payload wouldn't catch
        // that regression.
        byte[] originalPlaintext = new byte[37_000];
        new Random(20260822).nextBytes(originalPlaintext);
        int chunkSize = 4096;

        JsonNode issueItem = postJson(bulkUrl("/dek/issue"), authHeaders(),
                itemsBody(obj().put("key", "test"))).get("items").get(0);
        assertSuccess(issueItem, "dek/issue");
        UUID edekId = UUID.fromString(issueItem.get("edek_id").asText());
        byte[] dek = TransportWrapper.unwrap(
                Base64.getDecoder().decode(issueItem.get("wrapped_dek_b64").asText()), testPrivateKey);

        // Mirrors FileBulkJob.encryptOneFile's literal per-chunk logic --
        // that method is private, see class javadoc for why this doesn't
        // call it directly.
        List<byte[]> ivs = new ArrayList<>();
        List<byte[]> tags = new ArrayList<>();
        List<byte[]> ciphertexts = new ArrayList<>();
        for (int offset = 0; offset < originalPlaintext.length; offset += chunkSize) {
            byte[] chunk = Arrays.copyOfRange(originalPlaintext, offset, Math.min(offset + chunkSize, originalPlaintext.length));
            // Marker byte (see FileBulkJob's class javadoc) -- 0x00, this test
            // doesn't use compress-before-encrypt, but encryptOneFile always
            // writes the marker regardless, so mirroring it here keeps this
            // test honest about the real current wire format.
            byte[] marked = new byte[1 + chunk.length];
            marked[0] = 0x00;
            System.arraycopy(chunk, 0, marked, 1, chunk.length);
            String base64Plaintext = Base64.getEncoder().encodeToString(marked);
            DekManager.EncryptResult enc = DekManager.encrypt(base64Plaintext.getBytes(StandardCharsets.UTF_8), dek, APP_ID);
            ivs.add(enc.iv());
            tags.add(enc.tag());
            ciphertexts.add(enc.ciphertext());
        }
        assertTrue(ivs.size() > 1, "test must exercise multiple chunks, not a trivial single-chunk case");

        // The actual thing under test: FileBulkJob's real, public method -- not a reimplementation.
        ArrayNode items = MAPPER.createArrayNode();
        for (int i = 0; i < ivs.size(); i++) {
            String token = FileBulkJob.reconstructCoreServiceToken(edekId, ivs.get(i), tags.get(i), ciphertexts.get(i));
            items.add(obj().put("key", String.valueOf(i)).put("ciphertext", token));
        }
        JsonNode decryptResp = postJson(coreUrl("/decrypt/batch"), authHeaders(), itemsBody(items));

        Map<Integer, byte[]> byKey = new HashMap<>();
        for (JsonNode item : decryptResp.get("items")) {
            assertSuccess(item, "decrypt/batch item " + item.get("key"));
            byte[] marked = Base64.getDecoder().decode(item.get("result").get("plaintext").asText());
            byKey.put(Integer.parseInt(item.get("key").asText()), Arrays.copyOfRange(marked, 1, marked.length));
        }
        ByteArrayOutputStream reassembled = new ByteArrayOutputStream();
        for (int i = 0; i < ivs.size(); i++) {
            reassembled.write(byKey.get(i));
        }

        assertArrayEquals(originalPlaintext, reassembled.toByteArray(),
                "hsm-core-service /decrypt did not reproduce the original bytes for a hsm-bulk-service-encrypted "
                        + "chunk set -- interoperability regression");
    }

    @Test
    void coreServiceEncryptedValue_decryptsViaBulkServiceUnwrap() throws Exception {
        byte[] originalPlaintext = new byte[500];
        new Random(20260823).nextBytes(originalPlaintext);
        String base64Plaintext = Base64.getEncoder().encodeToString(originalPlaintext);

        ArrayNode encryptItems = MAPPER.createArrayNode()
                .add(obj().put("key", "test").put("plaintext", base64Plaintext).put("encoding", "base64"));
        JsonNode encryptItem = postJson(coreUrl("/encrypt/batch"), fullViewHeaders(), itemsBody(encryptItems))
                .get("items").get(0);
        assertSuccess(encryptItem, "encrypt/batch");
        UUID edekId = UUID.fromString(encryptItem.get("result").get("edek_id").asText());
        String token = encryptItem.get("result").get("ciphertext").asText();

        JsonNode unwrapItem = postJson(bulkUrl("/dek/unwrap"), authHeaders(),
                itemsBody(obj().put("key", "test").put("edek_id", edekId.toString()))).get("items").get(0);
        assertSuccess(unwrapItem, "dek/unwrap");
        byte[] dek = TransportWrapper.unwrap(
                Base64.getDecoder().decode(unwrapItem.get("wrapped_dek_b64").asText()), testPrivateKey);

        DekManager.UnpackedToken unpacked = DekManager.unpackToken(token);
        byte[] decrypted = DekManager.decrypt(unpacked.ciphertext(), unpacked.tag(), unpacked.iv(), dek, APP_ID);
        byte[] decoded = Base64.getDecoder().decode(new String(decrypted, StandardCharsets.UTF_8));

        assertArrayEquals(originalPlaintext, decoded,
                "local decrypt (via hsm-bulk-service's /dek/unwrap) did not reproduce the original bytes for a "
                        + "hsm-core-service-encrypted value -- interoperability regression");
    }

    @Test
    void tamperedReconstructedToken_failsCoreServiceDecrypt() throws Exception {
        byte[] chunk = "authenticity, not just format, must survive reconstruction".getBytes(StandardCharsets.UTF_8);
        String base64Plaintext = Base64.getEncoder().encodeToString(chunk);

        JsonNode issueItem = postJson(bulkUrl("/dek/issue"), authHeaders(),
                itemsBody(obj().put("key", "test"))).get("items").get(0);
        assertSuccess(issueItem, "dek/issue");
        UUID edekId = UUID.fromString(issueItem.get("edek_id").asText());
        byte[] dek = TransportWrapper.unwrap(
                Base64.getDecoder().decode(issueItem.get("wrapped_dek_b64").asText()), testPrivateKey);

        DekManager.EncryptResult enc = DekManager.encrypt(base64Plaintext.getBytes(StandardCharsets.UTF_8), dek, APP_ID);
        byte[] tamperedCiphertext = enc.ciphertext().clone();
        tamperedCiphertext[0] ^= 0x01; // flip one bit -- must invalidate the AEAD tag

        String token = FileBulkJob.reconstructCoreServiceToken(edekId, enc.iv(), enc.tag(), tamperedCiphertext);
        JsonNode item = postJson(coreUrl("/decrypt/batch"), authHeaders(),
                itemsBody(obj().put("key", "test").put("ciphertext", token))).get("items").get(0);

        assertEquals("error", item.get("status").asText(),
                "hsm-core-service accepted a tampered reconstructed token instead of rejecting it -- "
                        + "AEAD integrity check regression");
    }

    @Test
    void compressedChunks_decryptCorrectlyBothLocallyAndViaCoreService() throws Exception {
        // Genuinely compressible content this time (not random bytes) --
        // gzip needs real redundancy to actually shrink, mirroring
        // FileBulkJob's compress-before-encrypt path end to end, not just
        // exercising the marker byte with content that happens to not shrink.
        byte[] originalPlaintext = "the quick brown fox jumps over the lazy dog. ".repeat(2000)
                .getBytes(StandardCharsets.UTF_8);
        int chunkSize = 4096;

        JsonNode issueItem = postJson(bulkUrl("/dek/issue"), authHeaders(),
                itemsBody(obj().put("key", "test"))).get("items").get(0);
        assertSuccess(issueItem, "dek/issue");
        UUID edekId = UUID.fromString(issueItem.get("edek_id").asText());
        byte[] dek = TransportWrapper.unwrap(
                Base64.getDecoder().decode(issueItem.get("wrapped_dek_b64").asText()), testPrivateKey);

        // Mirrors FileBulkJob.encryptOneFile's compress-before-encrypt path
        // exactly -- that method is private, see class javadoc for why this
        // doesn't call it directly.
        List<byte[]> ivs = new ArrayList<>();
        List<byte[]> tags = new ArrayList<>();
        List<byte[]> ciphertexts = new ArrayList<>();
        int uncompressedTotal = 0;
        int compressedTotal = 0;
        for (int offset = 0; offset < originalPlaintext.length; offset += chunkSize) {
            byte[] chunk = Arrays.copyOfRange(originalPlaintext, offset, Math.min(offset + chunkSize, originalPlaintext.length));
            byte[] gzipped = gzip(chunk);
            uncompressedTotal += chunk.length;
            compressedTotal += gzipped.length;
            byte[] marked = new byte[1 + gzipped.length];
            marked[0] = 0x01;
            System.arraycopy(gzipped, 0, marked, 1, gzipped.length);
            String base64Plaintext = Base64.getEncoder().encodeToString(marked);
            DekManager.EncryptResult enc = DekManager.encrypt(base64Plaintext.getBytes(StandardCharsets.UTF_8), dek, APP_ID);
            ivs.add(enc.iv());
            tags.add(enc.tag());
            ciphertexts.add(enc.ciphertext());
        }
        assertTrue(ivs.size() > 1, "test must exercise multiple chunks, not a trivial single-chunk case");
        assertTrue(compressedTotal < uncompressedTotal,
                "test content must actually compress smaller, or this test isn't exercising anything real");

        // LOCAL decrypt path -- mirrors FileBulkJob.decryptOneFile's marker-byte handling.
        ByteArrayOutputStream localReassembled = new ByteArrayOutputStream();
        for (int i = 0; i < ivs.size(); i++) {
            byte[] plaintext = DekManager.decrypt(ciphertexts.get(i), tags.get(i), ivs.get(i), dek, APP_ID);
            byte[] marked = Base64.getDecoder().decode(new String(plaintext, StandardCharsets.UTF_8));
            byte flag = marked[0];
            byte[] payload = Arrays.copyOfRange(marked, 1, marked.length);
            localReassembled.write(flag == 0x01 ? gunzip(payload) : payload);
        }
        assertArrayEquals(originalPlaintext, localReassembled.toByteArray(),
                "local decrypt of compressed chunks did not reproduce the original bytes -- "
                        + "compress-before-encrypt regression");

        // REMOTE decrypt path -- real hsm-core-service /decrypt/batch, via the real reconstructCoreServiceToken.
        ArrayNode items = MAPPER.createArrayNode();
        for (int i = 0; i < ivs.size(); i++) {
            String token = FileBulkJob.reconstructCoreServiceToken(edekId, ivs.get(i), tags.get(i), ciphertexts.get(i));
            items.add(obj().put("key", String.valueOf(i)).put("ciphertext", token));
        }
        JsonNode decryptResp = postJson(coreUrl("/decrypt/batch"), authHeaders(), itemsBody(items));

        Map<Integer, byte[]> byKey = new HashMap<>();
        for (JsonNode item : decryptResp.get("items")) {
            assertSuccess(item, "decrypt/batch item " + item.get("key"));
            byte[] marked = Base64.getDecoder().decode(item.get("result").get("plaintext").asText());
            byte flag = marked[0];
            byte[] payload = Arrays.copyOfRange(marked, 1, marked.length);
            byKey.put(Integer.parseInt(item.get("key").asText()), flag == 0x01 ? gunzip(payload) : payload);
        }
        ByteArrayOutputStream remoteReassembled = new ByteArrayOutputStream();
        for (int i = 0; i < ivs.size(); i++) {
            remoteReassembled.write(byKey.get(i));
        }

        assertArrayEquals(originalPlaintext, remoteReassembled.toByteArray(),
                "hsm-core-service /decrypt did not reproduce the original bytes for compressed chunks -- "
                        + "compression + interoperability regression");
    }

    // --- helpers ---

    private static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
            gzip.write(data);
        }
        return compressed.toByteArray();
    }

    private static byte[] gunzip(byte[] data) throws IOException {
        ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
        try (GZIPInputStream gunzip = new GZIPInputStream(new ByteArrayInputStream(data))) {
            gunzip.transferTo(decompressed);
        }
        return decompressed.toByteArray();
    }

    private static ObjectNode obj() {
        return MAPPER.createObjectNode();
    }

    private static ObjectNode itemsBody(ObjectNode singleItem) {
        return obj().set("items", MAPPER.createArrayNode().add(singleItem));
    }

    private static ObjectNode itemsBody(ArrayNode items) {
        return obj().set("items", items);
    }

    private static String coreUrl(String path) {
        return "http://localhost:" + corePort + API_V1_PREFIX + path;
    }

    private static String bulkUrl(String path) {
        return "http://localhost:" + bulkPort + API_V1_PREFIX + path;
    }

    private static Map<String, String> authHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer " + DEMO_TOKEN);
        headers.put("X-App-ID", APP_ID);
        headers.put("X-Response-Detail", "minimal");
        return headers;
    }

    private static Map<String, String> fullViewHeaders() {
        Map<String, String> headers = new HashMap<>(authHeaders());
        headers.put("X-Response-Detail", "full");
        return headers;
    }

    private static JsonNode postJson(String url, Map<String, String> headers, ObjectNode body) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)));
        headers.forEach(builder::header);
        HttpResponse<String> resp = HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("POST " + url + " returned HTTP " + resp.statusCode() + ": " + resp.body());
        }
        return MAPPER.readTree(resp.body());
    }

    private static void assertSuccess(JsonNode item, String context) {
        if (!"success".equals(item.get("status").asText())) {
            fail(context + " failed: " + item.get("detail"));
        }
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void waitForPort(int port, Duration timeout) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("localhost", port), 500);
                return;
            } catch (IOException e) {
                Thread.sleep(500);
            }
        }
        throw new IllegalStateException("port " + port + " did not open within " + timeout);
    }

    private static Process startJar(Path jar, Path logFile, Map<String, String> env) throws IOException {
        String javaHome = System.getProperty("java.home");
        ProcessBuilder pb = new ProcessBuilder(javaHome + "/bin/java", "-jar", jar.toString());
        pb.environment().putAll(env);
        pb.redirectErrorStream(true);
        pb.redirectOutput(logFile.toFile());
        return pb.start();
    }
}
