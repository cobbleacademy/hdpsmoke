package com.hsm.client.file;

import com.hsm.client.config.ClientProperties;
import com.hsm.client.crypto.DekManager;
import com.hsm.client.crypto.TransportWrapper;
import com.hsm.client.fileformat.EncryptedFileException;
import com.hsm.client.fileformat.EncryptedFileFormat;
import com.hsm.client.fileformat.EncryptedFileReader;
import com.hsm.client.fileformat.EncryptedFileWriter;
import com.hsm.client.svc.SvcClient;
import com.hsm.client.svc.SvcConfig;
import org.slf4j.Logger;
import com.hsm.filestore.AdlsFileStore;
import com.hsm.filestore.AzureBlobFileStore;
import com.hsm.filestore.FileStore;
import com.hsm.filestore.LocalFileStore;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * BULK File job: one DEK per whole file by default, each chunk encrypted separately
 * under that DEK (DekManager.encrypt draws a fresh random IV per call, so this
 * needs no new crypto design). Chunks are stitched into a single output file via
 * length-prefixed binary framing -- immune to ciphertext content, unlike a newline
 * delimiter.
 *
 * <p>Wire format: owned by hsm-crypto-client's {@link EncryptedFileWriter}/
 * {@link EncryptedFileReader} (normative spec: java/docs/FILE_FORMAT.md) -- this class
 * no longer frames bytes itself, so it can never drift from hsm-file-service, which
 * reads the same files through the same code. v1 (default): [16-byte edek_id] then
 * repeated [4-byte length][iv(12) + tag(16) + ciphertext], each chunk's plaintext
 * base64(marker + chunk) -- the base64 layer keeps any single chunk decryptable via
 * hsm-core-service's /decrypt, whose response is a UTF-8 string. v2
 * (config.formatVersion() = 2): a "HSMF" header adding file_id and chunk_size, and
 * each chunk additionally carries file_id, its index, a final flag and the chunk
 * size inside the encrypted plaintext -- so truncation, reordering, splicing and
 * header-stripping are detected, with the AAD (and therefore core's /decrypt)
 * unchanged. Decrypt detects the version per file; nothing to configure.
 *
 * <p>config.compressBeforeEncrypt() (default false) gzips each chunk before the base64
 * step; the marker byte inside the authenticated plaintext records it per chunk, so
 * decrypt never needs a matching setting.
 *
 * <p>config.resultsEnabled() -- after each encrypt batch, a JSON-lines file recording
 * path, file_id, edek_id and sizes per file (see FileResultsWriter); a consumer loads
 * these to know each file's expected file_id.
 *
 * <p>Two decrypt paths resolve to the identical plaintext bytes, by design: LOCAL
 * (what this class's own decryptRange/decryptOneFile does) reads edek_id once,
 * resolves the DEK via SVC's /dek/unwrap, and decrypts every frame directly. REMOTE
 * -- for a consumer that never talks to hsm-bulk-service at all -- takes edek_id
 * (the file header) plus any one frame's iv/tag/ciphertext and calls {@link
 * #reconstructCoreServiceToken} to rebuild the exact ciphertext string
 * hsm-core-service's own /encrypt produces, then hands that straight to
 * hsm-core-service's unchanged {@code POST /decrypt}. Neither hsm-core-service's nor
 * hsm-bulk-service's own API changes for this -- reconstruction is a client-side
 * concern, one small pure function, not a new capability either service needs to
 * grow.
 *
 * <p>config.dekName() set -- one persistent DEK for the whole job (resolved once,
 * reused across every future run using the same name), instead of each file
 * minting its own. config.parallelism() &gt; 1 -- partitions the file list into
 * that many groups and runs one worker per group concurrently (files are
 * independent, unordered units, so this needs no key-range-style boundary math
 * the way DbBulkJob's row partitioning does). config.checkpoint().enabled() --
 * tracks completed files via FileCheckpointStore's single batched manifest, so a
 * crashed/killed run can resume instead of reprocessing everything.
 */
public class FileBulkJob {

    private static final Logger log = LoggerFactory.getLogger(FileBulkJob.class);

    private final ClientProperties.File config;
    private final SvcConfig svcConfig;
    private final SvcClient svcClient;
    private final PrivateKey privateKey;
    private final FileStore sourceStore;
    private final FileStore targetStore;
    private final FileCheckpointStore checkpointStore;
    // Shared across every partition worker for the lifetime of one decrypt() call,
    // same reasoning and same config-driven gate as DbBulkJob's decrypt-side DEK
    // cache: only populated/consulted when config.dekName() is set on the decrypt
    // job's own config (a job-level, not per-file, signal here -- File jobs have no
    // per-column granularity like DB's ColumnMapping does). Unset config.dekName()
    // means every file's DEK is genuinely one-off by design -- unchanged per-batch
    // behavior, no persistent cache, no benefit to caching a one-off value anyway.
    private final Map<UUID, OwnedFileDek> namedDekCache;
    private final EncryptedFileWriter.Options writeOptions;
    private final FileResultsWriter resultsWriter;

    public FileBulkJob(ClientProperties.File config, SvcConfig svcConfig, SvcClient svcClient) {
        this.config = config;
        this.svcConfig = svcConfig;
        this.svcClient = svcClient;
        this.privateKey = TransportWrapper.parsePrivateKeyPem(svcConfig.privateKeyPem());
        this.sourceStore = buildStore(config.source());
        this.targetStore = buildStore(config.target());
        this.checkpointStore = checkpointEnabled(config) ? new FileCheckpointStore() : null;
        this.namedDekCache = isNamed(config) ? new ConcurrentHashMap<>() : null;
        this.writeOptions = new EncryptedFileWriter.Options(
                config.formatVersion() == 2 ? EncryptedFileFormat.Version.V2 : EncryptedFileFormat.Version.V1,
                config.chunkSizeBytes(), config.compressBeforeEncrypt());
        this.resultsWriter = config.resultsEnabled()
                ? new FileResultsWriter(targetStore, checkpointEnabled(config) ? config.checkpoint().jobId() : null)
                : null;
    }

    private static boolean checkpointEnabled(ClientProperties.File config) {
        return config.checkpoint() != null && config.checkpoint().enabled();
    }

    private boolean checkpointEnabled() {
        return checkpointStore != null;
    }

    private static FileStore buildStore(ClientProperties.File.StoreRef ref) {
        return switch (ref.type()) {
            case LOCAL -> new LocalFileStore(ref.root());
            case ADLS -> new AdlsFileStore(ref.root(), ref.accountKey());
            case AZURE_BLOB -> new AzureBlobFileStore(ref.root(), ref.accountKey());
        };
    }

    private static boolean isNamed(ClientProperties.File config) {
        return config.dekName() != null && !config.dekName().isBlank();
    }

    /**
     * ownerAppId is the record's permanent owner -- NOT necessarily
     * svcConfig.appId() once a grant-authorized cross-app dek_name reuse is
     * in play. Must be used as the AES-GCM AAD, never svcConfig.appId()
     * directly -- see SvcClient.IssueResult's javadoc for the full reasoning
     * and the confirmed bug this field exists to avoid (same fix as
     * DbBulkJob's NamedDek, just for this class's own per-job DEK).
     */
    private record NamedFileDek(UUID edekId, String ownerAppId, byte[] dek) {
    }

    /** Decrypt-side cache/lookup entry -- same ownerAppId reasoning as NamedFileDek. */
    private record OwnedFileDek(String ownerAppId, byte[] dek) {
    }

    /** One /dek/issue call for the whole job when config.dekName() is set -- resolved once, shared read-only across every worker. */
    private NamedFileDek resolveJobDek() {
        if (!isNamed(config)) {
            return null;
        }
        List<SvcClient.IssueResult> issued = svcClient.issue(
                List.of(new SvcClient.IssueItem(config.dekName(), null, config.dekName())));
        SvcClient.IssueResult r = issued.get(0);
        if (!"success".equals(r.status())) {
            throw new IllegalStateException("dek/issue failed for dek-name=" + r.key() + ": " + r.detail());
        }
        byte[] dek = TransportWrapper.unwrap(Base64.getDecoder().decode(r.wrappedDekB64()), privateKey);
        log.info("file_bulk_named_dek_resolved dek_name={} reused={} owner_app_id={}", r.key(), r.reused(), r.ownerAppId());
        return new NamedFileDek(r.edekId(), r.ownerAppId(), dek);
    }

    /** Loads prior progress (resume=true) or clears it for a fresh start (resume=false); no-op entirely when checkpointing is disabled. */
    private Set<String> resolveCheckpointStart() {
        if (!checkpointEnabled()) {
            return Set.of();
        }
        ClientProperties.File.Checkpoint cp = config.checkpoint();
        if (!cp.resume()) {
            checkpointStore.clear();
            return Set.of();
        }
        Set<String> loaded = checkpointStore.loadCompleted(targetStore, cp.jobId());
        if (!loaded.isEmpty()) {
            log.info("file_bulk_resume job_id={} already_done={}", cp.jobId(), loaded.size());
        }
        return loaded;
    }

    public void encrypt() {
        List<String> files = sourceStore.list(config.fileTypes());
        log.info("file_bulk_encrypt_start file_count={} format_version={} chunk_size_bytes={} results_dir={}",
                files.size(), config.formatVersion(), config.chunkSizeBytes(),
                resultsWriter == null ? "disabled" : resultsWriter.runDir());

        NamedFileDek namedDek = resolveJobDek();
        Set<String> alreadyDone = resolveCheckpointStart();
        long startMs = System.currentTimeMillis();
        AtomicLong doneCounter = new AtomicLong();
        try {
            runPartitioned(files, "encrypt", (slice, workerId) -> encryptSlice(slice, namedDek, alreadyDone, workerId, doneCounter));
            logCompletion("encrypt", doneCounter.get(), startMs);
        } finally {
            if (namedDek != null) {
                DekManager.zeroDek(namedDek.dek());
            }
        }
    }

    public void decrypt() {
        List<String> files = sourceStore.list(config.fileTypes());
        log.info("file_bulk_decrypt_start file_count={}", files.size());

        Set<String> alreadyDone = resolveCheckpointStart();
        long startMs = System.currentTimeMillis();
        AtomicLong doneCounter = new AtomicLong();
        try {
            runPartitioned(files, "decrypt", (slice, workerId) -> decryptSlice(slice, alreadyDone, workerId, doneCounter));
            logCompletion("decrypt", doneCounter.get(), startMs);
        } finally {
            if (namedDekCache != null) {
                namedDekCache.values().forEach(owned -> DekManager.zeroDek(owned.dek()));
            }
        }
    }

    private void logCompletion(String direction, long totalFiles, long startMs) {
        long elapsedMs = System.currentTimeMillis() - startMs;
        double filesPerSec = elapsedMs > 0 ? totalFiles * 1000.0 / elapsedMs : 0;
        log.info("file_bulk_{}_complete total_files={} elapsed_ms={} files_per_sec={}",
                direction, totalFiles, elapsedMs, String.format("%.1f", filesPerSec));
    }

    @FunctionalInterface
    private interface SliceWorker {
        void run(List<String> slice, String workerId);
    }

    /** parallelism &lt;= 1 (default): runs inline on the whole list, identical to before parallelism existed. parallelism &gt; 1: splits the (independent, unordered) file list into that many groups and runs one worker per group concurrently. */
    private void runPartitioned(List<String> files, String direction, SliceWorker worker) {
        int parallelism = Math.max(1, config.parallelism());
        if (parallelism <= 1) {
            worker.run(files, checkpointEnabled() ? config.checkpoint().jobId() : null);
            return;
        }

        List<List<String>> groups = splitIntoGroups(files, parallelism);
        log.info("file_bulk_{}_parallel_start partitions={}", direction, groups.size());
        ExecutorService pool = Executors.newFixedThreadPool(groups.size());
        try {
            List<Future<?>> futures = new ArrayList<>();
            String jobId = checkpointEnabled() ? config.checkpoint().jobId() : null;
            for (List<String> group : groups) {
                futures.add(pool.submit(() -> worker.run(group, jobId)));
            }
            RuntimeException firstFailure = null;
            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    RuntimeException wrapped = new IllegalStateException("parallel worker failed: " + e.getCause(), e.getCause());
                    if (firstFailure == null) {
                        firstFailure = wrapped;
                    } else {
                        firstFailure.addSuppressed(wrapped);
                    }
                }
            }
            if (firstFailure != null) {
                throw firstFailure;
            }
        } finally {
            pool.shutdown();
        }
    }

    /** Even, contiguous split -- files are independent, unordered work items, so no boundary math (unlike DbBulkJob's key-range partitioning) is needed. */
    private static List<List<String>> splitIntoGroups(List<String> files, int groups) {
        int actualGroups = Math.max(1, Math.min(groups, files.size()));
        List<List<String>> result = new ArrayList<>();
        int base = files.size() / actualGroups;
        int remainder = files.size() % actualGroups;
        int start = 0;
        for (int g = 0; g < actualGroups; g++) {
            int size = base + (g < remainder ? 1 : 0);
            result.add(files.subList(start, start + size));
            start += size;
        }
        return result;
    }

    private void encryptSlice(List<String> slice, NamedFileDek namedDek, Set<String> alreadyDone, String jobId, AtomicLong doneCounter) {
        // Same reasoning as DbBulkJob's decrypt sub-chunking fix: dekBatchMaxItems
        // exists purely to bound the size of a real /dek/issue call. When namedDek
        // is already resolved (whole job shares one DEK, see resolveJobDek), no
        // per-batch network call ever happens at all -- every file in the batch
        // uses namedDek.dek() directly -- so capping the batch size for that reason
        // is pure overhead (more, smaller batches: more log lines, more small map
        // allocations) with zero benefit. Only cap by dekBatchMaxItems when each
        // file genuinely needs its own /dek/issue item.
        int filesPerCall = namedDek != null
                ? Math.max(1, config.filesPerBatch())
                : Math.max(1, Math.min(config.filesPerBatch(), svcConfig.dekBatchMaxItems()));
        long sinceFlush = 0;
        for (List<String> batch : partition(slice, filesPerCall)) {
            List<String> toProcess = batch.stream().filter(p -> !alreadyDone.contains(p)).toList();
            if (toProcess.isEmpty()) {
                continue;
            }

            List<FileResultsWriter.Entry> batchResults = new ArrayList<>(toProcess.size());
            if (namedDek != null) {
                for (String path : toProcess) {
                    batchResults.add(new FileResultsWriter.Entry(path,
                            encryptOneFile(path, namedDek.edekId(), namedDek.dek(), namedDek.ownerAppId())));
                    onFileDone(path, jobId, doneCounter);
                }
            } else {
                List<SvcClient.IssueItem> issueItems = toProcess.stream()
                        .map(path -> new SvcClient.IssueItem(path, null, null))
                        .toList();
                List<SvcClient.IssueResult> issued = svcClient.issue(issueItems);
                Map<String, SvcClient.IssueResult> byKey = new LinkedHashMap<>();
                for (SvcClient.IssueResult r : issued) {
                    byKey.put(r.key(), r);
                }
                for (String path : toProcess) {
                    SvcClient.IssueResult result = byKey.get(path);
                    if (result == null || !"success".equals(result.status())) {
                        throw new IllegalStateException("dek/issue failed for file " + path
                                + ": " + (result == null ? "no result returned" : result.detail()));
                    }
                    byte[] dek = TransportWrapper.unwrap(Base64.getDecoder().decode(result.wrappedDekB64()), privateKey);
                    try {
                        batchResults.add(new FileResultsWriter.Entry(path,
                                encryptOneFile(path, result.edekId(), dek, result.ownerAppId())));
                        onFileDone(path, jobId, doneCounter);
                    } finally {
                        DekManager.zeroDek(dek);
                    }
                }
            }
            // Written before the checkpoint flush below, so a file marked done always
            // has its result line on storage -- a resumed run never loses a file_id.
            if (resultsWriter != null) {
                resultsWriter.writeBatch(batchResults);
            }
            sinceFlush += toProcess.size();
            if (checkpointEnabled() && sinceFlush >= config.checkpoint().flushInterval()) {
                checkpointStore.flush(targetStore, jobId);
                sinceFlush = 0;
            }
            log.info("file_bulk_encrypt_progress job_id={} files_done={}", jobId, doneCounter.get());
        }
        if (checkpointEnabled()) {
            checkpointStore.flush(targetStore, jobId);
        }
    }

    private void onFileDone(String path, String jobId, AtomicLong doneCounter) {
        doneCounter.incrementAndGet();
        if (checkpointEnabled()) {
            checkpointStore.markDone(path);
        }
    }

    private EncryptedFileWriter.Result encryptOneFile(String relativePath, UUID edekId, byte[] dek, String ownerAppId) {
        try (InputStream in = sourceStore.openRead(relativePath);
             OutputStream out = targetStore.openWrite(relativePath)) {
            return EncryptedFileWriter.write(in, out, edekId, dek, ownerAppId, writeOptions);
        } catch (EncryptedFileException e) {
            throw new IllegalStateException("Failed to encrypt file " + relativePath + ": " + e.getMessage(), e);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encrypt file " + relativePath, e);
        }
    }

    private void decryptSlice(List<String> slice, Set<String> alreadyDone, String jobId, AtomicLong doneCounter) {
        // Same reasoning as encryptSlice above and as DbBulkJob's decrypt
        // sub-chunking fix. File dek-name is job-wide (see isNamed(ClientProperties.
        // File)), not per-file, so when namedDekCache is active every file in the
        // job shares exactly one edek_id -- distinctEdekIds is always size 1
        // regardless of batch size, even on the very first batch, so
        // dekBatchMaxItems never bounds anything real here and shouldn't shrink
        // the batch.
        int filesPerCall = namedDekCache != null
                ? Math.max(1, config.filesPerBatch())
                : Math.max(1, Math.min(config.filesPerBatch(), svcConfig.dekBatchMaxItems()));
        long sinceFlush = 0;
        for (List<String> batch : partition(slice, filesPerCall)) {
            List<String> toProcess = batch.stream().filter(p -> !alreadyDone.contains(p)).toList();
            if (toProcess.isEmpty()) {
                continue;
            }

            Map<String, UUID> edekIdByPath = new LinkedHashMap<>();
            for (String path : toProcess) {
                edekIdByPath.put(path, readEdekIdHeader(path));
            }
            // Dedup by edek_id, not by file -- many files can share one id under a
            // named (dek-name) DEK, same reasoning as DbBulkJob's decrypt path. One
            // /dek/unwrap call AND one local RSA-OAEP unwrap per distinct id, not one
            // per file.
            List<UUID> distinctEdekIds = edekIdByPath.values().stream().distinct().toList();

            Map<UUID, OwnedFileDek> dekByEdekId = new LinkedHashMap<>();
            if (namedDekCache != null) {
                for (UUID id : distinctEdekIds) {
                    OwnedFileDek cached = namedDekCache.get(id);
                    if (cached != null) {
                        dekByEdekId.put(id, cached);
                    }
                }
            }
            List<UUID> toFetch = distinctEdekIds.stream().filter(id -> !dekByEdekId.containsKey(id)).toList();
            List<SvcClient.UnwrapItem> unwrapItems = toFetch.stream()
                    .map(id -> new SvcClient.UnwrapItem(id.toString(), id))
                    .toList();
            List<SvcClient.UnwrapResult> unwrapped = unwrapItems.isEmpty() ? List.of() : svcClient.unwrap(unwrapItems);
            Map<UUID, SvcClient.UnwrapResult> resultByEdekId = new LinkedHashMap<>();
            for (SvcClient.UnwrapResult r : unwrapped) {
                resultByEdekId.put(UUID.fromString(r.key()), r);
            }

            try {
                for (Map.Entry<UUID, SvcClient.UnwrapResult> e : resultByEdekId.entrySet()) {
                    if ("success".equals(e.getValue().status())) {
                        byte[] dek = TransportWrapper.unwrap(
                                Base64.getDecoder().decode(e.getValue().wrappedDekB64()), privateKey);
                        OwnedFileDek owned = new OwnedFileDek(e.getValue().ownerAppId(), dek);
                        dekByEdekId.put(e.getKey(), owned);
                        if (namedDekCache != null) {
                            namedDekCache.put(e.getKey(), owned);
                        }
                    }
                }

                for (String path : toProcess) {
                    UUID edekId = edekIdByPath.get(path);
                    OwnedFileDek owned = dekByEdekId.get(edekId);
                    if (owned == null) {
                        SvcClient.UnwrapResult result = resultByEdekId.get(edekId);
                        throw new IllegalStateException("dek/unwrap failed for file " + path
                                + ": " + (result == null ? "no result returned" : result.detail()));
                    }
                    decryptOneFile(path, owned.dek(), owned.ownerAppId());
                    onFileDone(path, jobId, doneCounter);
                }
            } finally {
                // Only zero DEKs NOT retained in namedDekCache -- those are the same
                // byte[] instances the cache holds for future batches, zeroing them
                // here would corrupt the cache for later reuse.
                for (Map.Entry<UUID, OwnedFileDek> e : dekByEdekId.entrySet()) {
                    if (namedDekCache == null || !namedDekCache.containsKey(e.getKey())) {
                        DekManager.zeroDek(e.getValue().dek());
                    }
                }
            }
            sinceFlush += toProcess.size();
            if (checkpointEnabled() && sinceFlush >= config.checkpoint().flushInterval()) {
                checkpointStore.flush(targetStore, jobId);
                sinceFlush = 0;
            }
            log.info("file_bulk_decrypt_progress job_id={} files_done={}", jobId, doneCounter.get());
        }
        if (checkpointEnabled()) {
            checkpointStore.flush(targetStore, jobId);
        }
    }

    private UUID readEdekIdHeader(String relativePath) {
        try (InputStream in = sourceStore.openRead(relativePath)) {
            return EncryptedFileFormat.readHeader(in).edekId();
        } catch (EncryptedFileException e) {
            throw new IllegalStateException("Failed to read header of " + relativePath + ": " + e.getMessage(), e);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read edek_id header from " + relativePath, e);
        }
    }

    private void decryptOneFile(String relativePath, byte[] dek, String ownerAppId) {
        try (InputStream in = sourceStore.openRead(relativePath);
             OutputStream out = targetStore.openWrite(relativePath)) {
            EncryptedFileReader.open(in).decryptTo(out, dek, ownerAppId);
        } catch (EncryptedFileException e) {
            // Integrity failures (tampered, truncated, reordered, spliced, downgraded) and
            // limit breaches both stop the job: a bulk decrypt must never leave a
            // silently wrong file behind. The partially written target is left in place
            // for inspection; it is not a valid decryption.
            throw new IllegalStateException("Failed to decrypt " + relativePath + " (" + e.reason() + "): " + e.getMessage(), e);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to decrypt file " + relativePath, e);
        }
    }

    private static List<List<String>> partition(List<String> items, int size) {
        List<List<String>> result = new ArrayList<>();
        for (int i = 0; i < items.size(); i += size) {
            result.add(items.subList(i, Math.min(i + size, items.size())));
        }
        return result;
    }

    /**
     * The REMOTE decrypt path -- see class javadoc. Rebuilds the exact ciphertext
     * token string hsm-core-service's own /encrypt produces from one frame's
     * (edek_id, iv, tag, ciphertext) -- the same edek_id every frame of one file
     * shares, plus that one frame's own iv/tag/ciphertext read straight out of the
     * binary layout above. Hand the result to hsm-core-service's unchanged
     * {@code POST /decrypt} as the request's {@code ciphertext} field; the
     * returned plaintext is still base64-encoded (this class's own
     * plaintext-safety encoding, see the class javadoc), with the
     * compressed/raw marker byte as its first decoded byte -- decode, read
     * that byte, gzip-decompress the rest only if it's {@code 0x01} --
     * recovers the original raw chunk bytes, same as decryptOneFile does
     * locally.
     *
     * <p>Not called anywhere in this class -- decryptRange/decryptOneFile always
     * take the local path via SVC's /dek/unwrap. This exists purely as a public
     * capability for a consumer that wants to decrypt via hsm-core-service
     * directly instead, without ever talking to hsm-bulk-service. Works for v1 and
     * v2 alike (the AAD is identical); for v2, run each decrypted chunk through
     * {@code ChunkPayload.decode} -- or use
     * {@code EncryptedFileReader.Session.decryptTo(out, ChunkDecryptor)}, which also
     * enforces the final-chunk rules -- so the rescue path keeps v2's integrity checks.
     */
    public static String reconstructCoreServiceToken(UUID edekId, byte[] iv, byte[] tag, byte[] ciphertext) {
        return DekManager.packToken(edekId, iv, tag, ciphertext);
    }
}
