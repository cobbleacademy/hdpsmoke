package com.hsm.client.fileformat;

import com.hsm.client.config.FipsBootstrap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.Random;
import java.util.UUID;

/**
 * Fixed inputs for the committed golden files under src/test/resources/golden/, and
 * the one-off generator that produced them. The files are committed bytes, not
 * regenerated per build (IVs and file_ids are random), so any reader in any language
 * can be checked against the same ciphertext. Regenerate only when the format itself
 * changes -- and then the version must change too.
 *
 * <p>Run: {@code java -cp <test-classpath> com.hsm.client.fileformat.GoldenVectors src/test/resources/golden}
 */
public final class GoldenVectors {

    /** Test-only key -- never a real DEK. Bytes 0x00..0x1f. */
    static final byte[] DEK = HexFormat.of().parseHex("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");
    static final UUID EDEK_ID = UUID.fromString("5f1c0e2a-7b3d-4c8e-9a61-2d4b8f0c3e7a");
    static final String OWNER_APP_ID = "golden-app";
    static final int CHUNK_SIZE = 4096;
    static final int PLAINTEXT_SIZE = 10_000; // 3 chunks: 4096 + 4096 + 1808

    static final String[] FILES = {"v1-raw.bin", "v1-gzip.bin", "v2-raw.bin", "v2-gzip.bin", "v2-empty.bin"};

    private GoldenVectors() {
    }

    static byte[] plaintext() {
        byte[] b = new byte[PLAINTEXT_SIZE];
        new Random(20260926L).nextBytes(b);
        return b;
    }

    public static void main(String[] args) throws Exception {
        FipsBootstrap.register();
        Path dir = Path.of(args.length > 0 ? args[0] : "src/test/resources/golden");
        Files.createDirectories(dir);
        byte[] pt = plaintext();
        Files.write(dir.resolve("plaintext.bin"), pt);
        write(dir, "v1-raw.bin", pt, EncryptedFileWriter.Options.v1(CHUNK_SIZE, false));
        write(dir, "v1-gzip.bin", pt, EncryptedFileWriter.Options.v1(CHUNK_SIZE, true));
        write(dir, "v2-raw.bin", pt, EncryptedFileWriter.Options.v2(CHUNK_SIZE, false));
        write(dir, "v2-gzip.bin", pt, EncryptedFileWriter.Options.v2(CHUNK_SIZE, true));
        write(dir, "v2-empty.bin", new byte[0], EncryptedFileWriter.Options.v2(CHUNK_SIZE, false));
    }

    private static void write(Path dir, String name, byte[] pt, EncryptedFileWriter.Options options) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        EncryptedFileWriter.write(new ByteArrayInputStream(pt), out, EDEK_ID, DEK, OWNER_APP_ID, options);
        Files.write(dir.resolve(name), out.toByteArray());
    }
}
