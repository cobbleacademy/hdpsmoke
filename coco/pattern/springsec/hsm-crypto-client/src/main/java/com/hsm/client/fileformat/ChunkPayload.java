package com.hsm.client.fileformat;

import com.hsm.client.fileformat.EncryptedFileException.Reason;
import com.hsm.client.fileformat.EncryptedFileFormat.FileHeader;
import com.hsm.client.fileformat.EncryptedFileFormat.Version;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Encodes and decodes the plaintext that sits inside one encrypted chunk (the bytes
 * AES-GCM protects), and enforces the v1/v2 binding rules on the way out.
 *
 * <p>Public on purpose: a rescue caller that decrypts chunks through
 * hsm-core-service's {@code POST /decrypt} (instead of locally) gets back exactly the
 * base64 text {@link #encode} produced, and must run it through {@link #decode} to get
 * the same integrity checks a local read gets.
 */
public final class ChunkPayload {

    static final byte V1_RAW = 0x00;
    static final byte V1_GZIP = 0x01;
    static final byte V2_RAW = 0x02;
    static final byte V2_GZIP = 0x03;

    /** file_id(16) + chunk_index(8) + is_final(1) + chunk_size(4). */
    public static final int V2_BINDING_BYTES = 16 + 8 + 1 + 4;

    private ChunkPayload() {
    }

    /** One decoded chunk: the original bytes, and whether the writer flagged it final (always false for v1, which has no flag). */
    public record Decoded(byte[] payload, boolean isFinal) {
    }

    /** Builds the base64 UTF-8 bytes to encrypt for one chunk. */
    public static byte[] encode(FileHeader header, long chunkIndex, boolean isFinal, byte[] raw, boolean compress) {
        byte[] payload = compress ? gzip(raw) : raw;
        ByteBuffer buf;
        if (header.version() == Version.V1) {
            buf = ByteBuffer.allocate(1 + payload.length);
            buf.put(compress ? V1_GZIP : V1_RAW);
        } else {
            buf = ByteBuffer.allocate(1 + V2_BINDING_BYTES + payload.length);
            buf.put(compress ? V2_GZIP : V2_RAW);
            buf.putLong(header.fileId().getMostSignificantBits());
            buf.putLong(header.fileId().getLeastSignificantBits());
            buf.putLong(chunkIndex);
            buf.put(isFinal ? (byte) 1 : (byte) 0);
            buf.putInt(header.chunkSizeBytes());
        }
        buf.put(payload);
        return Base64.getEncoder().encode(buf.array());
    }

    /**
     * Decodes one decrypted chunk and applies every per-chunk rule for the header's
     * version. Sequence rules that span chunks (final flag position, trailing data)
     * are enforced by the caller walking the frames -- see
     * {@link EncryptedFileReader.Session#decryptTo}.
     *
     * @param base64Plaintext the decrypted chunk: UTF-8 base64 text, exactly as AES-GCM (or core's /decrypt) returned it
     * @param expectedIndex   0-based position of this frame in the file
     * @param maxChunkBytes   hard cap on the decoded (and decompressed) payload, whatever the header claims
     */
    public static Decoded decode(FileHeader header, long expectedIndex, byte[] base64Plaintext, int maxChunkBytes) {
        byte[] marked;
        try {
            marked = Base64.getDecoder().decode(base64Plaintext);
        } catch (IllegalArgumentException e) {
            throw new EncryptedFileException(Reason.MALFORMED, "chunk " + expectedIndex + " is not valid base64");
        }
        if (marked.length == 0) {
            throw new EncryptedFileException(Reason.MALFORMED, "chunk " + expectedIndex + " is empty");
        }
        byte marker = marked[0];
        boolean v2Marker = marker == V2_RAW || marker == V2_GZIP;
        boolean v1Marker = marker == V1_RAW || marker == V1_GZIP;
        if (!v1Marker && !v2Marker) {
            throw new EncryptedFileException(Reason.MALFORMED,
                    String.format("chunk %d has unknown marker 0x%02x", expectedIndex, marker));
        }
        if (header.version() == Version.V1 && v2Marker) {
            // The marker is inside the authenticated plaintext, so it can't be forged:
            // a v2 chunk in a v1-looking file means the v2 header was stripped.
            throw new EncryptedFileException(Reason.VERSION_MISMATCH,
                    "chunk " + expectedIndex + " is a v2 chunk but the file header is v1 (header stripped / downgrade attempt)");
        }
        if (header.version() == Version.V2 && v1Marker) {
            throw new EncryptedFileException(Reason.VERSION_MISMATCH,
                    "chunk " + expectedIndex + " is a v1 chunk but the file header is v2");
        }
        boolean gzip = marker == V1_GZIP || marker == V2_GZIP;

        if (header.version() == Version.V1) {
            byte[] payload = slice(marked, 1);
            return new Decoded(gzip ? gunzip(payload, maxChunkBytes, expectedIndex) : checkSize(payload, maxChunkBytes, expectedIndex), false);
        }

        if (marked.length < 1 + V2_BINDING_BYTES) {
            throw new EncryptedFileException(Reason.MALFORMED, "chunk " + expectedIndex + " is too short for v2 binding fields");
        }
        ByteBuffer buf = ByteBuffer.wrap(marked, 1, V2_BINDING_BYTES);
        UUID fileId = new UUID(buf.getLong(), buf.getLong());
        long index = buf.getLong();
        byte finalFlag = buf.get();
        int chunkSize = buf.getInt();
        if (!fileId.equals(header.fileId())) {
            throw new EncryptedFileException(Reason.FILE_ID_MISMATCH,
                    "chunk " + expectedIndex + " belongs to a different file (spliced)");
        }
        if (index != expectedIndex) {
            throw new EncryptedFileException(Reason.CHUNK_OUT_OF_ORDER,
                    "chunk at position " + expectedIndex + " carries index " + index + " (reordered, duplicated or dropped)");
        }
        if (finalFlag != 0 && finalFlag != 1) {
            throw new EncryptedFileException(Reason.MALFORMED, "chunk " + expectedIndex + " has invalid is_final flag");
        }
        if (chunkSize != header.chunkSizeBytes()) {
            throw new EncryptedFileException(Reason.HEADER_MISMATCH,
                    "chunk " + expectedIndex + " chunk_size " + chunkSize + " does not match header " + header.chunkSizeBytes());
        }
        int cap = Math.min(maxChunkBytes, header.chunkSizeBytes());
        byte[] encoded = slice(marked, 1 + V2_BINDING_BYTES);
        byte[] payload = gzip ? gunzip(encoded, cap, expectedIndex) : checkSize(encoded, cap, expectedIndex);
        boolean isFinal = finalFlag == 1;
        if (!isFinal && payload.length != header.chunkSizeBytes()) {
            throw new EncryptedFileException(Reason.HEADER_MISMATCH,
                    "non-final chunk " + expectedIndex + " holds " + payload.length + " bytes, expected " + header.chunkSizeBytes());
        }
        return new Decoded(payload, isFinal);
    }

    /** UTF-8 convenience for rescue callers holding core's /decrypt response as a String. */
    public static Decoded decode(FileHeader header, long expectedIndex, String base64Plaintext, int maxChunkBytes) {
        return decode(header, expectedIndex, base64Plaintext.getBytes(StandardCharsets.US_ASCII), maxChunkBytes);
    }

    private static byte[] checkSize(byte[] payload, int max, long index) {
        if (payload.length > max) {
            throw new EncryptedFileException(Reason.LIMIT_EXCEEDED,
                    "chunk " + index + " payload " + payload.length + " bytes exceeds limit " + max);
        }
        return payload;
    }

    private static byte[] slice(byte[] b, int from) {
        byte[] out = new byte[b.length - from];
        System.arraycopy(b, from, out, 0, out.length);
        return out;
    }

    private static byte[] gzip(byte[] data) {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream(data.length / 2 + 64);
        try (GZIPOutputStream gz = new GZIPOutputStream(compressed)) {
            gz.write(data);
        } catch (IOException e) {
            throw new EncryptedFileException(Reason.IO, "gzip failed", e);
        }
        return compressed.toByteArray();
    }

    /** Bounded decompression: never inflates past max bytes, so a crafted chunk can't exhaust memory (gzip bomb). */
    private static byte[] gunzip(byte[] data, int max, long index) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[64 * 1024];
        try (GZIPInputStream gz = new GZIPInputStream(new ByteArrayInputStream(data))) {
            int r;
            long total = 0;
            while ((r = gz.read(buf)) > 0) {
                total += r;
                if (total > max) {
                    throw new EncryptedFileException(Reason.LIMIT_EXCEEDED,
                            "chunk " + index + " decompresses beyond limit " + max);
                }
                out.write(buf, 0, r);
            }
        } catch (IOException e) {
            throw new EncryptedFileException(Reason.MALFORMED, "chunk " + index + " has invalid gzip data", e);
        }
        return out.toByteArray();
    }
}
