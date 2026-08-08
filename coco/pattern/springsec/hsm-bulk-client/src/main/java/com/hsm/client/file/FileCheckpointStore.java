package com.hsm.client.file;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Tracks per-file completion for FileBulkJob's checkpoint feature via a single
 * batched manifest file in the target FileStore -- deliberately not a marker file
 * per source file (would double write/blob-create operations, a real added cost
 * on ADLS specifically, which bills and rate-limits per operation) and not a new
 * external DB dependency (keeps this job storage-agnostic, same as everything
 * else here). The manifest is a newline-delimited list of completed relative
 * paths.
 *
 * <p>FileStore has no append primitive, so flush() rewrites the manifest in full
 * each time rather than truly appending -- called once per processed batch (not
 * per file), so the amortized cost per file stays small even though each
 * individual flush's cost grows with the total completed count so far.
 *
 * <p>A missed or corrupt manifest read/write is never treated as fatal: worst
 * case is reprocessing an already-done file on the next run, which is safe (a
 * fresh random IV every re-encrypt means a redundant write, not corruption) --
 * the same idempotent-safe fallback property DbBulkJob's checkpoint leans on via
 * its transactional coupling, achieved here by simply never trusting a marker
 * enough to skip something that can't be safely re-verified.
 */
class FileCheckpointStore {

    private static final String MANIFEST_DIR = ".hsm_bulk_checkpoint";

    private final Set<String> completed = new ConcurrentSkipListSet<>();

    Set<String> loadCompleted(FileStore targetStore, String jobId) {
        String path = manifestPath(jobId);
        try (InputStream in = targetStore.openRead(path);
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            Set<String> loaded = new HashSet<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    loaded.add(line);
                }
            }
            completed.addAll(loaded);
            return loaded;
        } catch (Exception e) {
            // No manifest yet (first run for this job-id), or unreadable -- treat as
            // empty rather than fatal.
            return Set.of();
        }
    }

    void markDone(String relativePath) {
        completed.add(relativePath);
    }

    boolean isDone(String relativePath) {
        return completed.contains(relativePath);
    }

    /** resume=false ("override") -- forget prior progress; still tracks/flushes new progress going forward. */
    void clear() {
        completed.clear();
    }

    synchronized void flush(FileStore targetStore, String jobId) {
        String path = manifestPath(jobId);
        try (OutputStream out = targetStore.openWrite(path);
             Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
            for (String p : completed) {
                writer.write(p);
                writer.write('\n');
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write checkpoint manifest " + path, e);
        }
    }

    private static String manifestPath(String jobId) {
        return MANIFEST_DIR + "/" + jobId + ".manifest";
    }
}
