package com.hsm.client.fileformat;

/**
 * Every way reading an encrypted file can fail, as one exception type with a
 * machine-readable {@link Reason} -- so callers (hsm-bulk-client, hsm-file-service,
 * rescue tooling) can map a failure to a stable error code without parsing messages.
 * Messages never contain key material or plaintext.
 */
public class EncryptedFileException extends RuntimeException {

    public enum Reason {
        /** Header or frame bytes are structurally invalid (bad lengths, bad base64, bad marker). */
        MALFORMED,
        /** Stream ended mid-frame, or (v2) ended before the chunk flagged final. */
        TRUNCATED,
        /** (v2) Bytes follow the chunk flagged final. */
        TRAILING_DATA,
        /** (v2) A chunk's embedded index doesn't match its position: reordered, duplicated or dropped. */
        CHUNK_OUT_OF_ORDER,
        /** (v2) A chunk's embedded file_id doesn't match the header: spliced in from another file. */
        FILE_ID_MISMATCH,
        /** (v2) A chunk's embedded chunk_size doesn't match the header, or a non-final chunk is short. */
        HEADER_MISMATCH,
        /** Header version and chunk marker disagree -- e.g. a v2 chunk inside a v1-looking file (header stripped: downgrade attempt). */
        VERSION_MISMATCH,
        /** AES-GCM tag verification failed: ciphertext tampered, wrong key, or wrong owner app_id. */
        AUTH_FAILED,
        /** A frame or decompressed chunk exceeds the configured limits (defends against memory exhaustion). */
        LIMIT_EXCEEDED,
        /** Reading the encrypted input failed (storage/network), not a format problem. */
        IO,
        /** Writing decrypted bytes to the caller's OutputStream failed -- typically the HTTP client went away. */
        OUTPUT_IO
    }

    private final Reason reason;

    public EncryptedFileException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public EncryptedFileException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }

    /** True for every reason that means "these bytes are not an authentic, complete file" -- as opposed to limits or I/O. */
    public boolean isIntegrityFailure() {
        return switch (reason) {
            case TRUNCATED, TRAILING_DATA, CHUNK_OUT_OF_ORDER, FILE_ID_MISMATCH, HEADER_MISMATCH,
                 VERSION_MISMATCH, AUTH_FAILED, MALFORMED -> true;
            case LIMIT_EXCEEDED, IO, OUTPUT_IO -> false;
        };
    }
}
