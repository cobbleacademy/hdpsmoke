package com.hsm.client.fileformat;

import com.hsm.client.crypto.DekManager;
import com.hsm.client.fileformat.EncryptedFileException.Reason;
import com.hsm.client.fileformat.EncryptedFileFormat.FileHeader;
import com.hsm.client.fileformat.EncryptedFileFormat.Version;

import javax.crypto.AEADBadTagException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Streams an encrypted file (v1 or v2) back to its original bytes, one chunk at a
 * time, verifying every chunk before its bytes are written to the output. For v2 the
 * whole-file rules -- chunk order, exactly one final chunk, nothing after it -- are
 * enforced as frames go by, so truncation is detected at the point the stream ends.
 *
 * <p>Two-step on purpose: {@link #open} reads only the header, so a caller can check
 * the file_id (e.g. hsm-file-service's X-Expected-File-Id) and resolve the DEK by
 * edek_id before any chunk is decrypted.
 */
public final class EncryptedFileReader {

    private EncryptedFileReader() {
    }

    /**
     * Hard caps applied regardless of what a (possibly tampered) header claims.
     * maxFrameBytes bounds the allocation for one frame; maxChunkPlaintextBytes bounds
     * one chunk's decoded/decompressed size. v1 files record no chunk size, so these
     * are their only bound.
     */
    public record Limits(int maxFrameBytes, int maxChunkPlaintextBytes) {
        /** Library default: accepts any bulk-client chunk size up to 48 MiB. Long-running services should set tighter values. */
        public static final Limits DEFAULT = new Limits(64 * 1024 * 1024, 48 * 1024 * 1024);

        public Limits {
            if (maxFrameBytes <= EncryptedFileFormat.FRAME_OVERHEAD_BYTES || maxChunkPlaintextBytes <= 0) {
                throw new IllegalArgumentException("limits must be positive");
            }
        }
    }

    /** One frame as stored: position in the file plus the AES-GCM pieces. */
    public record Frame(long index, byte[] iv, byte[] tag, byte[] ciphertext) {
    }

    /**
     * Turns one frame into the chunk's decrypted base64 plaintext. The local
     * implementation is AES-GCM with the unwrapped DEK; a rescue implementation can
     * instead send {@link #toCoreServiceToken} to hsm-core-service's /decrypt.
     */
    @FunctionalInterface
    public interface ChunkDecryptor {
        byte[] decrypt(FileHeader header, Frame frame) throws AEADBadTagException;
    }

    public record ReadResult(long chunkCount, long plaintextBytes) {
    }

    public static Session open(InputStream in) {
        return open(in, Limits.DEFAULT);
    }

    public static Session open(InputStream in, Limits limits) {
        FileHeader header = EncryptedFileFormat.readHeader(in);
        if (header.version() == Version.V2 && header.chunkSizeBytes() > limits.maxChunkPlaintextBytes()) {
            throw new EncryptedFileException(Reason.LIMIT_EXCEEDED,
                    "chunk_size " + header.chunkSizeBytes() + " exceeds limit " + limits.maxChunkPlaintextBytes());
        }
        return new Session(in, header, limits);
    }

    /** Rebuilds the exact "v1.…" token hsm-core-service's /encrypt would have produced for this chunk -- the rescue path. */
    public static String toCoreServiceToken(FileHeader header, Frame frame) {
        return DekManager.packToken(header.edekId(), frame.iv(), frame.tag(), frame.ciphertext());
    }

    public static final class Session {
        private final InputStream in;
        private final FileHeader header;
        private final Limits limits;
        private final long maxFrameBytes;
        private long nextIndex;

        private Session(InputStream in, FileHeader header, Limits limits) {
            this.in = in;
            this.header = header;
            this.limits = limits;
            this.maxFrameBytes = header.version() == Version.V2
                    ? Math.min(limits.maxFrameBytes(), EncryptedFileFormat.maxV2FrameBytes(header.chunkSizeBytes()))
                    : limits.maxFrameBytes();
        }

        public FileHeader header() {
            return header;
        }

        /** Local decrypt with an already-unwrapped DEK. ownerAppId is the AAD -- the DEK's owner from /dek/unwrap. */
        public ReadResult decryptTo(OutputStream out, byte[] dek, String ownerAppId) {
            return decryptTo(out, (h, f) -> DekManager.decrypt(f.ciphertext(), f.tag(), f.iv(), dek, ownerAppId));
        }

        /**
         * Decrypts, verifies and writes every chunk in order. Each chunk's bytes reach
         * {@code out} only after that chunk has passed every check, but earlier chunks
         * have already been written when a later one fails -- a caller that must never
         * release a partial file should decrypt into a buffer first (hsm-file-service
         * does this below its size threshold).
         */
        public ReadResult decryptTo(OutputStream out, ChunkDecryptor decryptor) {
            long plaintextBytes = 0;
            boolean finalSeen = false;
            Frame frame;
            while ((frame = nextFrame()) != null) {
                if (finalSeen) {
                    throw new EncryptedFileException(Reason.TRAILING_DATA,
                            "data follows the final chunk (chunk " + frame.index() + ")");
                }
                byte[] base64Plaintext;
                try {
                    base64Plaintext = decryptor.decrypt(header, frame);
                } catch (AEADBadTagException e) {
                    throw new EncryptedFileException(Reason.AUTH_FAILED,
                            "chunk " + frame.index() + " failed AES-GCM authentication (tampered, wrong key or wrong owner)", e);
                }
                ChunkPayload.Decoded decoded = ChunkPayload.decode(header, frame.index(), base64Plaintext,
                        limits.maxChunkPlaintextBytes());
                try {
                    out.write(decoded.payload());
                } catch (IOException e) {
                    throw new EncryptedFileException(Reason.OUTPUT_IO, "I/O error writing decrypted chunk " + frame.index(), e);
                }
                plaintextBytes += decoded.payload().length;
                finalSeen = decoded.isFinal();
            }
            if (header.version() == Version.V2 && !finalSeen) {
                throw new EncryptedFileException(Reason.TRUNCATED,
                        "stream ended after " + nextIndex + " chunk(s) without a final chunk (truncated)");
            }
            return new ReadResult(nextIndex, plaintextBytes);
        }

        /**
         * Next raw frame, or null at a clean end of stream (exactly on a frame
         * boundary). Exposed for rescue tools that decrypt frames remotely in batches;
         * such callers must still run each result through {@link ChunkPayload#decode}
         * and apply the final-chunk rules themselves -- or simply use
         * {@link #decryptTo(OutputStream, ChunkDecryptor)}, which does both.
         */
        public Frame nextFrame() {
            int first;
            try {
                first = in.read();
            } catch (IOException e) {
                throw new EncryptedFileException(Reason.IO, "I/O error reading frame " + nextIndex, e);
            }
            if (first < 0) {
                return null;
            }
            byte[] rest = EncryptedFileFormat.readExactly(in, 3, "frame " + nextIndex + " length");
            int length = (first << 24) | ((rest[0] & 0xff) << 16) | ((rest[1] & 0xff) << 8) | (rest[2] & 0xff);
            if (length <= EncryptedFileFormat.FRAME_OVERHEAD_BYTES) {
                throw new EncryptedFileException(Reason.MALFORMED, "frame " + nextIndex + " has invalid length " + length);
            }
            if (length > maxFrameBytes) {
                throw new EncryptedFileException(Reason.LIMIT_EXCEEDED,
                        "frame " + nextIndex + " length " + length + " exceeds limit " + maxFrameBytes);
            }
            byte[] body = EncryptedFileFormat.readExactly(in, length, "frame " + nextIndex);
            byte[] iv = new byte[DekManager.IV_LENGTH];
            byte[] tag = new byte[DekManager.TAG_LENGTH];
            byte[] ciphertext = new byte[length - EncryptedFileFormat.FRAME_OVERHEAD_BYTES];
            System.arraycopy(body, 0, iv, 0, iv.length);
            System.arraycopy(body, iv.length, tag, 0, tag.length);
            System.arraycopy(body, EncryptedFileFormat.FRAME_OVERHEAD_BYTES, ciphertext, 0, ciphertext.length);
            return new Frame(nextIndex++, iv, tag, ciphertext);
        }
    }
}
