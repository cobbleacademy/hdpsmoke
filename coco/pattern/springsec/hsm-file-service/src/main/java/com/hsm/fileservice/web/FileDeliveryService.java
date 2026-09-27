package com.hsm.fileservice.web;

import com.hsm.client.HsmCryptoClient;
import com.hsm.client.fileformat.EncryptedFileException;
import com.hsm.client.fileformat.EncryptedFileFormat.FileHeader;
import com.hsm.client.fileformat.EncryptedFileFormat.Version;
import com.hsm.client.fileformat.EncryptedFileReader;
import com.hsm.client.svc.SvcClient;
import com.hsm.fileservice.config.FileServiceProperties;
import com.hsm.filestore.FileStore;
import com.hsm.filestore.StoreFileNotFoundException;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.MediaTypeFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Reads one stored file, verifies it and writes the original bytes to the response.
 *
 * <p>Delivery mode is chosen per request from the stored (encrypted) size:
 * <ul>
 *   <li><b>buffered</b> (stored size &le; buffer-threshold-bytes and a buffer slot is free):
 *       the whole file is decrypted and verified in memory first, so the client gets
 *       either the complete file with a Content-Length, or a clean JSON error -- never
 *       partial content.</li>
 *   <li><b>streaming</b> (larger files, or no free slot): each chunk is sent as soon as
 *       it is verified. A failure before the first byte is still a clean JSON error; a
 *       failure after it aborts the connection (see {@link StreamAbortedException}).</li>
 * </ul>
 */
@Service
public class FileDeliveryService {

    public static final String EXPECTED_FILE_ID_HEADER = "X-Expected-File-Id";

    private final HsmCryptoClient crypto;
    private final FileStore store;
    private final EncryptedFileReader.Limits limits;
    private final Semaphore bufferSlots;
    private final FileServiceProperties.Delivery delivery;
    private final boolean requireExpectedFileId;

    public FileDeliveryService(HsmCryptoClient crypto, FileStore store, EncryptedFileReader.Limits limits,
                               Semaphore bufferSlots, FileServiceProperties props) {
        this.crypto = crypto;
        this.store = store;
        this.limits = limits;
        this.bufferSlots = bufferSlots;
        this.delivery = props.delivery();
        this.requireExpectedFileId = props.access().requireExpectedFileId();
    }

    void serve(String path, String expectedFileIdHeader, HttpServletResponse response, RequestTrace trace) {
        UUID expected = parseExpected(expectedFileIdHeader);
        if (expected == null && requireExpectedFileId) {
            throw new FileServiceException(ErrorCode.FILE_ID_REQUIRED, "no X-Expected-File-Id and access.require-expected-file-id=true");
        }
        long storedBytes = storedSize(path);

        try (InputStream in = new BufferedInputStream(openRead(path), 64 * 1024)) {
            EncryptedFileReader.Session session;
            try {
                session = crypto.openEncryptedFile(in, limits);
            } catch (RuntimeException e) {
                throw translate(e);
            }
            FileHeader header = session.header();
            trace.formatVersion = header.version() == Version.V2 ? "2" : "1";
            trace.fileId = header.fileId() == null ? null : header.fileId().toString();
            checkExpected(header, expected);

            boolean buffered = storedBytes <= delivery.bufferThresholdBytes() && acquireSlot();
            if (buffered) {
                trace.mode = "buffered";
                try {
                    deliverBuffered(session, path, storedBytes, response, trace);
                } finally {
                    bufferSlots.release();
                }
            } else {
                trace.mode = "streaming";
                deliverStreaming(session, path, response, trace);
            }
        } catch (IOException e) {
            // Only reachable from closing the storage stream after a completed delivery.
            throw new FileServiceException(ErrorCode.STORAGE, "error closing storage stream", e);
        }
    }

    private void deliverBuffered(EncryptedFileReader.Session session, String path, long storedBytes,
                                 HttpServletResponse response, RequestTrace trace) {
        ByteArrayOutputStream plaintext = new ByteArrayOutputStream((int) Math.min(Integer.MAX_VALUE - 16, storedBytes * 3 / 4 + 16));
        EncryptedFileReader.ReadResult result;
        try {
            result = crypto.decryptFile(session, plaintext);
        } catch (RuntimeException e) {
            throw translate(e);
        }
        setHeaders(response, path, session.header(), "buffered");
        response.setContentLengthLong(plaintext.size());
        try {
            ServletOutputStream out = response.getOutputStream();
            plaintext.writeTo(out);
            out.flush();
        } catch (IOException e) {
            trace.outcome = "client_closed";
            throw new StreamAbortedException(null, "client closed connection during buffered write", e);
        }
        trace.bytes = result.plaintextBytes();
    }

    private void deliverStreaming(EncryptedFileReader.Session session, String path,
                                  HttpServletResponse response, RequestTrace trace) {
        setHeaders(response, path, session.header(), "streaming");
        try {
            ServletOutputStream out = response.getOutputStream();
            EncryptedFileReader.ReadResult result = crypto.decryptFile(session, out);
            out.flush();
            trace.bytes = result.plaintextBytes();
        } catch (EncryptedFileException e) {
            if (e.reason() == EncryptedFileException.Reason.OUTPUT_IO) {
                trace.outcome = "client_closed";
                throw new StreamAbortedException(null, "client closed connection mid-stream", e);
            }
            throw failStream(response, translate(e));
        } catch (IOException e) {
            trace.outcome = "client_closed";
            throw new StreamAbortedException(null, "client closed connection mid-stream", e);
        } catch (RuntimeException e) {
            throw failStream(response, translate(e));
        }
    }

    /** Before the first byte: clean error. After it: abort the connection so the client can't mistake partial content for a whole file. */
    private static RuntimeException failStream(HttpServletResponse response, FileServiceException failure) {
        if (!response.isCommitted()) {
            response.reset();
            return failure;
        }
        return new StreamAbortedException(failure.code(), failure.getMessage(), failure);
    }

    private void setHeaders(HttpServletResponse response, String path, FileHeader header, String mode) {
        String filename = path.substring(path.lastIndexOf('/') + 1);
        MediaType type = MediaTypeFactory.getMediaType(filename).orElse(MediaType.APPLICATION_OCTET_STREAM);
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType(type.toString());
        response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
                ContentDisposition.builder(delivery.contentDisposition()).filename(filename, StandardCharsets.UTF_8).build().toString());
        response.setHeader(HttpHeaders.CACHE_CONTROL, "no-store");
        response.setHeader(HttpHeaders.PRAGMA, "no-cache");
        response.setHeader("X-Content-Type-Options", "nosniff");
        response.setHeader("X-HSM-Format-Version", header.version() == Version.V2 ? "2" : "1");
        response.setHeader("X-HSM-Delivery", mode);
        if (header.fileId() != null) {
            response.setHeader("X-HSM-File-Id", header.fileId().toString());
        }
    }

    private boolean acquireSlot() {
        try {
            return bufferSlots.tryAcquire(delivery.bufferAcquireTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static UUID parseExpected(String header) {
        if (header == null || header.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(header.strip());
        } catch (IllegalArgumentException e) {
            throw new FileServiceException(ErrorCode.BAD_FILE_ID, "unparseable X-Expected-File-Id");
        }
    }

    private static void checkExpected(FileHeader header, UUID expected) {
        if (expected == null) {
            return;
        }
        if (header.fileId() == null) {
            throw new FileServiceException(ErrorCode.NO_FILE_ID, "expected file id given but stored file is v1");
        }
        if (!header.fileId().equals(expected)) {
            throw new FileServiceException(ErrorCode.FILE_ID_MISMATCH,
                    "stored file_id " + header.fileId() + " != expected " + expected);
        }
    }

    private long storedSize(String path) {
        try {
            return store.size(path);
        } catch (StoreFileNotFoundException e) {
            throw new FileServiceException(ErrorCode.NOT_FOUND, "no stored file", e);
        } catch (IllegalArgumentException e) {
            throw new FileServiceException(ErrorCode.BAD_PATH, e.getMessage(), e);
        } catch (RuntimeException e) {
            throw new FileServiceException(ErrorCode.STORAGE, "stat failed: " + e.getMessage(), e);
        }
    }

    private InputStream openRead(String path) {
        try {
            return store.openRead(path);
        } catch (RuntimeException e) {
            throw new FileServiceException(ErrorCode.STORAGE, "open failed: " + e.getMessage(), e);
        }
    }

    /** Maps library failures to stable codes. The upstream message stays in the exception (logged), never in the response. */
    static FileServiceException translate(RuntimeException e) {
        if (e instanceof FileServiceException fse) {
            return fse;
        }
        if (e instanceof EncryptedFileException efe) {
            ErrorCode code = switch (efe.reason()) {
                case LIMIT_EXCEEDED -> ErrorCode.LIMIT;
                case IO -> ErrorCode.STORAGE;
                case OUTPUT_IO -> ErrorCode.INTERNAL;
                default -> ErrorCode.INTEGRITY;
            };
            return new FileServiceException(code, efe.reason() + ": " + efe.getMessage(), efe);
        }
        if (e instanceof HsmCryptoClient.HsmCryptoClientException) {
            // /dek/unwrap answered but refused: missing cross-app grant, key shredded, or wrong app registration.
            return new FileServiceException(ErrorCode.KEY_UNAVAILABLE, e.getMessage(), e);
        }
        if (e instanceof SvcClient.SvcClientException) {
            return new FileServiceException(ErrorCode.CORE_UNAVAILABLE, e.getMessage(), e);
        }
        return new FileServiceException(ErrorCode.INTERNAL, e.toString(), e);
    }
}
