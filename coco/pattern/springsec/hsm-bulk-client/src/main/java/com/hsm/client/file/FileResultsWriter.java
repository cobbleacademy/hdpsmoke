package com.hsm.client.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hsm.client.fileformat.EncryptedFileFormat;
import com.hsm.client.fileformat.EncryptedFileWriter;
import com.hsm.filestore.FileStore;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Writes one JSON-lines file per processed encrypt batch, recording what each file was
 * encrypted as -- above all its v2 file_id. A consumer loads these into its own
 * database so hsm-file-service can later be asked for a file together with the
 * file_id it is expected to have (X-Expected-File-Id), which is what detects a whole
 * file being swapped for another valid one. At billions of files one big manifest
 * would be unworkable; many small per-batch files can be ingested incrementally.
 *
 * <p>Layout: {@code <target>/.hsm_bulk_results/<job-id or "run">/<run-start>-<rand>/batch-000001.jsonl}.
 * The run-start segment keeps a resumed run from overwriting an earlier run's files.
 * FileStore.list() skips this directory, so a decrypt job pointed at the same target
 * never mistakes result files for data.
 *
 * <p>These files contain paths and ids, never keys or plaintext -- but a path can be
 * sensitive business data, so they belong to the same access tier as the encrypted
 * files next to them. They are an ingestion feed, not the security anchor: storing the
 * expected file_id only protects anything once it lives in a store an attacker with
 * storage write access cannot also change.
 */
final class FileResultsWriter {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter RUN_STAMP =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

    private final FileStore targetStore;
    private final String runDir;
    private final AtomicLong batchCounter = new AtomicLong();

    FileResultsWriter(FileStore targetStore, String jobId) {
        this.targetStore = targetStore;
        String job = jobId == null || jobId.isBlank() ? "run" : jobId;
        this.runDir = FileStore.RESULTS_DIR + "/" + job + "/"
                + RUN_STAMP.format(Instant.now()) + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    record Entry(String path, EncryptedFileWriter.Result result) {
    }

    /** Thread-safe: each call claims its own batch number. No-op for an empty batch. */
    void writeBatch(List<Entry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        String name = String.format("%s/batch-%06d.jsonl", runDir, batchCounter.incrementAndGet());
        String now = Instant.now().toString();
        try (OutputStream out = targetStore.openWrite(name)) {
            for (Entry e : entries) {
                EncryptedFileWriter.Result r = e.result();
                ObjectNode line = MAPPER.createObjectNode()
                        .put("path", e.path())
                        .put("file_id", r.fileId() == null ? null : r.fileId().toString())
                        .put("edek_id", r.edekId().toString())
                        .put("format_version", r.version() == EncryptedFileFormat.Version.V2 ? 2 : 1)
                        .put("chunk_size_bytes", r.chunkSizeBytes())
                        .put("chunk_count", r.chunkCount())
                        .put("plaintext_bytes", r.plaintextBytes())
                        .put("encrypted_bytes", r.encryptedBytes())
                        .put("encrypted_at", now);
                out.write(MAPPER.writeValueAsString(line).getBytes(StandardCharsets.UTF_8));
                out.write('\n');
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write results file " + name, e);
        }
    }

    String runDir() {
        return runDir;
    }
}
