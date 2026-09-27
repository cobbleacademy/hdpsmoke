package com.hsm.client.fileformat;

import com.hsm.client.config.FipsBootstrap;
import com.hsm.client.fileformat.EncryptedFileFormat.FileHeader;
import com.hsm.client.fileformat.EncryptedFileFormat.Version;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Decrypts the committed golden files. If this fails after a code change, the change
 * broke compatibility with files already written in the field -- fix the code, don't
 * regenerate the vectors.
 */
class GoldenVectorsTest {

    static {
        FipsBootstrap.register();
    }

    private static byte[] resource(String name) throws IOException {
        try (InputStream in = GoldenVectorsTest.class.getResourceAsStream("/golden/" + name)) {
            assertNotNull(in, "missing golden file " + name);
            return in.readAllBytes();
        }
    }

    private static byte[] decrypt(byte[] file) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        EncryptedFileReader.open(new ByteArrayInputStream(file))
                .decryptTo(out, GoldenVectors.DEK, GoldenVectors.OWNER_APP_ID);
        return out.toByteArray();
    }

    @Test
    void committedPlaintext_matchesGenerator() throws IOException {
        assertArrayEquals(GoldenVectors.plaintext(), resource("plaintext.bin"));
    }

    @Test
    void everyGoldenFile_decryptsToTheCommittedPlaintext() throws IOException {
        byte[] expected = resource("plaintext.bin");
        for (String name : GoldenVectors.FILES) {
            byte[] actual = decrypt(resource(name));
            assertArrayEquals(name.equals("v2-empty.bin") ? new byte[0] : expected, actual, name);
        }
    }

    @Test
    void goldenHeaders_parseAsExpected() throws IOException {
        for (String name : GoldenVectors.FILES) {
            FileHeader h = EncryptedFileFormat.readHeader(new ByteArrayInputStream(resource(name)));
            assertEquals(GoldenVectors.EDEK_ID, h.edekId(), name);
            if (name.startsWith("v1")) {
                assertEquals(Version.V1, h.version(), name);
                assertNull(h.fileId(), name);
            } else {
                assertEquals(Version.V2, h.version(), name);
                assertNotNull(h.fileId(), name);
                assertEquals(GoldenVectors.CHUNK_SIZE, h.chunkSizeBytes(), name);
            }
        }
    }
}
