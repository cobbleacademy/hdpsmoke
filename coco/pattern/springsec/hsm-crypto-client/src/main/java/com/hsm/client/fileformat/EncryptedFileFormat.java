package com.hsm.client.fileformat;

import com.hsm.client.crypto.DekManager;
import org.bouncycastle.crypto.CryptoServicesRegistrar;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

/**
 * The one definition of the chunked encrypted-file layout, shared by every reader and
 * writer (hsm-bulk-client, hsm-file-service, HsmCryptoClient embedders). The
 * normative, language-neutral spec is java/docs/FILE_FORMAT.md; if this class and
 * that document ever disagree, fix whichever is wrong -- golden vectors under
 * src/test/resources/golden/ pin the bytes.
 *
 * <pre>
 * v1 header (16 B):  edek_id
 * v2 header (41 B):  "HSMF" | 0x02 | edek_id(16) | file_id(16) | chunk_size(int32 BE)
 *
 * frames (both):     repeat { length(int32 BE) | iv(12) | tag(16) | ciphertext }
 *
 * ciphertext = AES-256-GCM(DEK, iv, aad = "hsm-svc:app_id=" + owner_app_id,
 *                          plaintext = UTF-8(base64(chunk_plaintext)))
 *
 * v1 chunk_plaintext: marker(0x00 raw | 0x01 gzip) | payload
 * v2 chunk_plaintext: marker(0x02 raw | 0x03 gzip) | file_id(16) | chunk_index(int64 BE)
 *                     | is_final(0x00|0x01) | chunk_size(int32 BE) | payload
 * </pre>
 *
 * <p>The v2 binding fields live inside the encrypted chunk rather than in the AAD, so
 * the AAD -- and therefore hsm-core-service's unchanged {@code POST /decrypt} -- stays
 * identical to v1: any single chunk of either version can still be decrypted by core
 * for rescue, with the binding checks then applied by {@link ChunkPayload} on the
 * caller's side.
 */
public final class EncryptedFileFormat {

    public enum Version { V1, V2 }

    static final byte[] MAGIC = {'H', 'S', 'M', 'F'};
    static final byte VERSION_2 = 0x02;

    public static final int V1_HEADER_BYTES = 16;
    public static final int V2_HEADER_BYTES = MAGIC.length + 1 + 16 + 16 + 4; // 41

    /** iv + tag -- the fixed part of every frame body. */
    public static final int FRAME_OVERHEAD_BYTES = DekManager.IV_LENGTH + DekManager.TAG_LENGTH;

    private EncryptedFileFormat() {
    }

    /**
     * Parsed file header. {@code fileId} and {@code chunkSizeBytes} are only present
     * for v2 (null / 0 for v1, which records neither).
     */
    public record FileHeader(Version version, UUID edekId, UUID fileId, int chunkSizeBytes) {
        public int headerBytes() {
            return version == Version.V1 ? V1_HEADER_BYTES : V2_HEADER_BYTES;
        }
    }

    /**
     * Reads and classifies the header. Detection: first 5 bytes == "HSMF" 0x02 means
     * v2; anything else is v1 (whose first 16 bytes are a random edek_id). The chunk
     * marker confirms the version later -- see {@link ChunkPayload}.
     */
    public static FileHeader readHeader(InputStream in) {
        byte[] first = readExactly(in, V1_HEADER_BYTES, "header");
        if (!startsWithV2Magic(first)) {
            return new FileHeader(Version.V1, uuid(first, 0), null, 0);
        }
        byte[] rest = readExactly(in, V2_HEADER_BYTES - V1_HEADER_BYTES, "v2 header");
        byte[] full = new byte[V2_HEADER_BYTES];
        System.arraycopy(first, 0, full, 0, first.length);
        System.arraycopy(rest, 0, full, first.length, rest.length);
        ByteBuffer buf = ByteBuffer.wrap(full, MAGIC.length + 1, V2_HEADER_BYTES - MAGIC.length - 1);
        UUID edekId = new UUID(buf.getLong(), buf.getLong());
        UUID fileId = new UUID(buf.getLong(), buf.getLong());
        int chunkSize = buf.getInt();
        if (chunkSize <= 0) {
            throw new EncryptedFileException(EncryptedFileException.Reason.MALFORMED,
                    "v2 header has invalid chunk_size " + chunkSize);
        }
        return new FileHeader(Version.V2, edekId, fileId, chunkSize);
    }

    static void writeHeader(DataOutputStream out, FileHeader header) throws IOException {
        if (header.version() == Version.V2) {
            out.write(MAGIC);
            out.writeByte(VERSION_2);
        }
        out.writeLong(header.edekId().getMostSignificantBits());
        out.writeLong(header.edekId().getLeastSignificantBits());
        if (header.version() == Version.V2) {
            out.writeLong(header.fileId().getMostSignificantBits());
            out.writeLong(header.fileId().getLeastSignificantBits());
            out.writeInt(header.chunkSizeBytes());
        }
    }

    private static boolean startsWithV2Magic(byte[] first) {
        return Arrays.equals(first, 0, MAGIC.length, MAGIC, 0, MAGIC.length) && first[MAGIC.length] == VERSION_2;
    }

    /**
     * Random version-4 UUID drawn from the BC-FIPS DRBG (the same source as keys and
     * IVs), not {@link UUID#randomUUID()}'s default SecureRandom. v4 rather than v7:
     * the header is not encrypted, and a v7 id would disclose each file's creation time.
     */
    public static UUID newFileId() {
        byte[] b = new byte[16];
        CryptoServicesRegistrar.getSecureRandom().nextBytes(b);
        b[6] = (byte) ((b[6] & 0x0f) | 0x40); // version 4
        b[8] = (byte) ((b[8] & 0x3f) | 0x80); // IETF variant
        return uuid(b, 0);
    }

    /** Largest legal frame body for a v2 file with this chunk size (raw or gzip payload), after base64. */
    static long maxV2FrameBytes(int chunkSizeBytes) {
        long maxPayload = gzipBound(chunkSizeBytes);
        return FRAME_OVERHEAD_BYTES + base64Length(1 + ChunkPayload.V2_BINDING_BYTES + maxPayload);
    }

    /** Conservative upper bound on gzip output for n input bytes (stored-block worst case plus header/trailer). */
    static long gzipBound(long n) {
        return n + (n >> 12) + 5 * (n / 16_383 + 1) + 64;
    }

    static long base64Length(long rawBytes) {
        return 4 * ((rawBytes + 2) / 3);
    }

    static UUID uuid(byte[] b, int offset) {
        ByteBuffer buf = ByteBuffer.wrap(b, offset, 16);
        return new UUID(buf.getLong(), buf.getLong());
    }

    static byte[] readExactly(InputStream in, int n, String what) {
        byte[] out = new byte[n];
        int off = 0;
        try {
            while (off < n) {
                int r = in.read(out, off, n - off);
                if (r < 0) {
                    throw new EncryptedFileException(EncryptedFileException.Reason.TRUNCATED,
                            "stream ended after " + off + " of " + n + " bytes reading " + what);
                }
                off += r;
            }
        } catch (IOException e) {
            throw new EncryptedFileException(EncryptedFileException.Reason.IO, "I/O error reading " + what, e);
        }
        return out;
    }
}
