package com.hsm.client.fileformat;

import com.hsm.client.crypto.DekManager;
import com.hsm.client.fileformat.EncryptedFileException.Reason;
import com.hsm.client.fileformat.EncryptedFileFormat.FileHeader;
import com.hsm.client.fileformat.EncryptedFileFormat.Version;

import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.UUID;

/**
 * Streams a plaintext file into the chunked encrypted layout (see
 * {@link EncryptedFileFormat}). Memory use is two chunks (the current one and, for v2,
 * one read-ahead to know which chunk is final) regardless of file size.
 *
 * <p>v1 output is byte-for-byte the layout FileBulkJob has always written (random
 * IVs aside), so existing v1 readers -- including hsm-core-service's /decrypt rescue
 * path -- keep working. v2 is opt-in until every reader is upgraded; see
 * java/docs/FILE_FORMAT.md "Rollout".
 */
public final class EncryptedFileWriter {

    /** Bulk-job default. For files a UI will open through hsm-file-service, 1 MiB is recommended -- see FILE_FORMAT.md "Chunk size". */
    public static final int DEFAULT_CHUNK_SIZE_BYTES = 8 * 1024 * 1024;

    private EncryptedFileWriter() {
    }

    public record Options(Version version, int chunkSizeBytes, boolean compress) {
        public Options {
            if (version == null) {
                version = Version.V1;
            }
            if (chunkSizeBytes <= 0) {
                chunkSizeBytes = DEFAULT_CHUNK_SIZE_BYTES;
            }
        }

        public static Options v1(int chunkSizeBytes, boolean compress) {
            return new Options(Version.V1, chunkSizeBytes, compress);
        }

        public static Options v2(int chunkSizeBytes, boolean compress) {
            return new Options(Version.V2, chunkSizeBytes, compress);
        }
    }

    /**
     * What was written. {@code fileId} is null for v1. Callers that want the
     * file-swap check (hsm-file-service's X-Expected-File-Id) must persist fileId
     * somewhere an attacker with storage write access cannot change -- typically the
     * consumer's own database.
     */
    public record Result(Version version, UUID edekId, UUID fileId, int chunkSizeBytes,
                         long chunkCount, long plaintextBytes, long encryptedBytes) {
    }

    /**
     * @param ownerAppId the DEK's owner as reported by /dek/issue -- the AES-GCM AAD,
     *                   never the caller's own app_id when a cross-app grant is in play
     */
    public static Result write(InputStream plaintext, OutputStream out, UUID edekId, byte[] dek,
                               String ownerAppId, Options options) {
        UUID fileId = options.version() == Version.V2 ? EncryptedFileFormat.newFileId() : null;
        FileHeader header = new FileHeader(options.version(), edekId, fileId,
                options.version() == Version.V2 ? options.chunkSizeBytes() : 0);
        CountingOutputStream counting = new CountingOutputStream(out);
        DataOutputStream data = new DataOutputStream(counting);
        long chunkCount = 0;
        long plaintextBytes = 0;
        try {
            EncryptedFileFormat.writeHeader(data, header);
            int size = options.chunkSizeBytes();
            byte[] current = readChunk(plaintext, size);
            if (header.version() == Version.V1) {
                // v1: no final flag, so no read-ahead; an empty file is header-only.
                while (current.length > 0) {
                    writeFrame(data, header, chunkCount++, false, current, dek, ownerAppId, options.compress());
                    plaintextBytes += current.length;
                    current = current.length < size ? new byte[0] : readChunk(plaintext, size);
                }
            } else {
                // v2: always at least one chunk, the last flagged final -- an empty file is
                // one empty final chunk, so truncating to header-only is detectable.
                while (true) {
                    byte[] next = current.length < size ? new byte[0] : readChunk(plaintext, size);
                    boolean isFinal = next.length == 0;
                    writeFrame(data, header, chunkCount++, isFinal, current, dek, ownerAppId, options.compress());
                    plaintextBytes += current.length;
                    if (isFinal) {
                        break;
                    }
                    current = next;
                }
            }
            data.flush();
        } catch (IOException e) {
            throw new EncryptedFileException(Reason.IO, "I/O error writing encrypted file", e);
        }
        return new Result(header.version(), edekId, fileId, options.chunkSizeBytes(),
                chunkCount, plaintextBytes, counting.count);
    }

    private static void writeFrame(DataOutputStream out, FileHeader header, long index, boolean isFinal, byte[] raw,
                                   byte[] dek, String ownerAppId, boolean compress) throws IOException {
        byte[] chunkPlaintext = ChunkPayload.encode(header, index, isFinal, raw, compress);
        DekManager.EncryptResult enc = DekManager.encrypt(chunkPlaintext, dek, ownerAppId);
        out.writeInt(EncryptedFileFormat.FRAME_OVERHEAD_BYTES + enc.ciphertext().length);
        out.write(enc.iv());
        out.write(enc.tag());
        out.write(enc.ciphertext());
    }

    /** Reads up to size bytes, blocking until the buffer is full or EOF -- so every non-final chunk is exactly size bytes. */
    private static byte[] readChunk(InputStream in, int size) throws IOException {
        byte[] buf = new byte[size];
        int off = 0;
        while (off < size) {
            int r = in.read(buf, off, size - off);
            if (r < 0) {
                break;
            }
            off += r;
        }
        return off == size ? buf : Arrays.copyOf(buf, off);
    }

    private static final class CountingOutputStream extends FilterOutputStream {
        long count;

        CountingOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            count++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            count += len;
        }
    }
}
