package com.hsm.client.file;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsm.client.config.ClientProperties;
import com.hsm.client.config.FipsBootstrap;
import com.hsm.client.crypto.DekManager;
import com.hsm.client.crypto.TransportWrapper;
import com.hsm.client.fileformat.EncryptedFileFormat;
import com.hsm.client.svc.SvcClient;
import com.hsm.client.svc.SvcConfig;
import com.hsm.filestore.FileStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FileBulkJob end to end on local disk with an in-memory SvcClient: format-version
 * selection, result files, and decrypt auto-detecting v1 and v2 in one run.
 */
class FileBulkJobFormatTest {

    static {
        FipsBootstrap.register();
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String APP = "payments-svc";

    @TempDir
    Path tmp;

    /** Issues fresh DEKs and unwraps previously issued ones -- the two calls FileBulkJob makes. */
    private static final class InMemorySvc extends SvcClient {
        final Map<UUID, byte[]> deks = new HashMap<>();
        final KeyPair transport;

        InMemorySvc(SvcConfig config, KeyPair transport) {
            super(config);
            this.transport = transport;
        }

        @Override
        public synchronized List<IssueResult> issue(List<IssueItem> items) {
            List<IssueResult> out = new ArrayList<>();
            for (IssueItem item : items) {
                UUID id = UUID.randomUUID();
                byte[] dek = DekManager.generateDek();
                deks.put(id, dek);
                out.add(new IssueResult(item.key(), "success", id, wrap(dek), APP, null, false));
            }
            return out;
        }

        @Override
        public synchronized List<UnwrapResult> unwrap(List<UnwrapItem> items) {
            List<UnwrapResult> out = new ArrayList<>();
            for (UnwrapItem item : items) {
                out.add(new UnwrapResult(item.key(), "success", item.edekId(), wrap(deks.get(item.edekId())), APP, null));
            }
            return out;
        }

        private String wrap(byte[] dek) {
            return Base64.getEncoder().encodeToString(TransportWrapper.wrap(dek, transport.getPublic()));
        }
    }

    private static KeyPair keyPair() throws Exception {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        return kpg.generateKeyPair();
    }

    private static SvcConfig svcConfig(KeyPair kp) {
        String pem = "-----BEGIN PRIVATE KEY-----\n" + Base64.getMimeEncoder().encodeToString(kp.getPrivate().getEncoded())
                + "\n-----END PRIVATE KEY-----\n";
        return new SvcConfig("http://unused.invalid", "/api/sensec/hsm/v1", APP, SvcConfig.AuthMode.STATIC,
                "unused", null, 100, pem, null, null, null, null);
    }

    private static ClientProperties.File fileConfig(Path source, Path target, int formatVersion, Boolean results) {
        return new ClientProperties.File(
                new ClientProperties.File.StoreRef(ClientProperties.File.StoreType.LOCAL, source.toString(), null),
                new ClientProperties.File.StoreRef(ClientProperties.File.StoreType.LOCAL, target.toString(), null),
                List.of(), 4096, 10, null, 1, null, false, formatVersion, results);
    }

    private Map<String, byte[]> seed(Path dir, int count) throws Exception {
        Map<String, byte[]> files = new HashMap<>();
        Random rnd = new Random(42);
        for (int i = 0; i < count; i++) {
            byte[] b = new byte[1000 + rnd.nextInt(20_000)];
            rnd.nextBytes(b);
            String rel = "docs/f" + i + ".bin";
            Files.createDirectories(dir.resolve("docs"));
            Files.write(dir.resolve(rel), b);
            files.put(rel, b);
        }
        return files;
    }

    private static List<JsonNode> resultLines(Path target) throws Exception {
        Path results = target.resolve(FileStore.RESULTS_DIR);
        if (!Files.exists(results)) {
            return List.of();
        }
        List<JsonNode> lines = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(results)) {
            for (Path p : walk.filter(Files::isRegularFile).toList()) {
                for (String line : Files.readAllLines(p)) {
                    lines.add(MAPPER.readTree(line));
                }
            }
        }
        return lines;
    }

    @Test
    void v2Encrypt_writesV2Files_andResultLinesMatchingEachHeader_thenDecryptsBack() throws Exception {
        KeyPair kp = keyPair();
        InMemorySvc svc = new InMemorySvc(svcConfig(kp), kp);
        Path plain = tmp.resolve("plain");
        Path enc = tmp.resolve("enc");
        Path dec = tmp.resolve("dec");
        Map<String, byte[]> originals = seed(plain, 5);

        new FileBulkJob(fileConfig(plain, enc, 2, null), svcConfig(kp), svc).encrypt();

        List<JsonNode> lines = resultLines(enc);
        assertEquals(5, lines.size(), "one result line per file (default on for v2)");
        for (JsonNode line : lines) {
            String path = line.get("path").asText();
            try (InputStream in = Files.newInputStream(enc.resolve(path))) {
                EncryptedFileFormat.FileHeader h = EncryptedFileFormat.readHeader(in);
                assertEquals(EncryptedFileFormat.Version.V2, h.version());
                assertEquals(h.fileId().toString(), line.get("file_id").asText());
                assertEquals(h.edekId().toString(), line.get("edek_id").asText());
            }
            assertEquals(2, line.get("format_version").asInt());
            assertEquals(originals.get(path).length, line.get("plaintext_bytes").asLong());
        }

        new FileBulkJob(fileConfig(enc, dec, 1, null), svcConfig(kp), svc).decrypt();
        for (Map.Entry<String, byte[]> e : originals.entrySet()) {
            assertArrayEquals(e.getValue(), Files.readAllBytes(dec.resolve(e.getKey())), e.getKey());
        }
        assertFalse(Files.exists(dec.resolve(FileStore.RESULTS_DIR)), "result files are never treated as data");
    }

    @Test
    void defaultIsV1_withNoResultFiles() throws Exception {
        KeyPair kp = keyPair();
        InMemorySvc svc = new InMemorySvc(svcConfig(kp), kp);
        Path plain = tmp.resolve("plain");
        Path enc = tmp.resolve("enc");
        seed(plain, 2);
        new FileBulkJob(fileConfig(plain, enc, 0, null), svcConfig(kp), svc).encrypt();
        assertTrue(resultLines(enc).isEmpty());
        try (InputStream in = Files.newInputStream(enc.resolve("docs/f0.bin"))) {
            assertEquals(EncryptedFileFormat.Version.V1, EncryptedFileFormat.readHeader(in).version());
        }
    }

    @Test
    void decrypt_handlesAMixOfV1AndV2InOneRun() throws Exception {
        KeyPair kp = keyPair();
        InMemorySvc svc = new InMemorySvc(svcConfig(kp), kp);
        Path plainA = tmp.resolve("a");
        Path plainB = tmp.resolve("b");
        Path enc = tmp.resolve("enc");
        Path dec = tmp.resolve("dec");
        Map<String, byte[]> a = seed(plainA, 2);
        Files.createDirectories(plainB.resolve("docs"));
        byte[] extra = new byte[5000];
        new Random(9).nextBytes(extra);
        Files.write(plainB.resolve("docs/v2.bin"), extra);

        new FileBulkJob(fileConfig(plainA, enc, 1, false), svcConfig(kp), svc).encrypt();
        new FileBulkJob(fileConfig(plainB, enc, 2, false), svcConfig(kp), svc).encrypt();
        new FileBulkJob(fileConfig(enc, dec, 1, null), svcConfig(kp), svc).decrypt();

        for (Map.Entry<String, byte[]> e : a.entrySet()) {
            assertArrayEquals(e.getValue(), Files.readAllBytes(dec.resolve(e.getKey())));
        }
        assertArrayEquals(extra, Files.readAllBytes(dec.resolve("docs/v2.bin")));
    }
}
