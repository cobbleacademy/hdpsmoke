package com.hsm.client.file;

import com.hsm.client.config.ClientProperties;
import com.hsm.client.crypto.DekManager;
import com.hsm.client.crypto.TransportWrapper;
import com.hsm.client.svc.SvcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import javax.crypto.AEADBadTagException;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Arrays;
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
 * <p>Output file layout: [16 bytes edek_id] then repeated [4-byte chunk length]
 * [iv(12) + tag(16) + ciphertext(N)] until EOF. No explicit chunk-count field (a
 * simplification from the original per-file header sketch): the framing is
 * self-terminating at end-of-stream, which avoids needing to know the chunk count
 * up front -- important for FileStore.openWrite's push-style streaming (in
 * particular AdlsFileStore's pipe-based upload), which can't seek back to fill in a
 * count after the fact.
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
    private final ClientProperties.Svc svcConfig;
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
    private final Map<UUID, byte[]> namedDekCache;

    public FileBulkJob(ClientProperties.File config, ClientProperties.Svc svcConfig, SvcClient svcClient) {
        this.config = config;
        this.svcConfig = svcConfig;
        this.svcClient = svcClient;
        this.privateKey = TransportWrapper.parsePrivateKeyPem(svcConfig.privateKeyPem());
        this.sourceStore = buildStore(config.source());
        this.targetStore = buildStore(config.target());
        this.checkpointStore = checkpointEnabled(config) ? new FileCheckpointStore() : null;
        this.namedDekCache = isNamed(config) ? new ConcurrentHashMap<>() : null;
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
            case ADLS -> new AdlsFileStore(ref.root());
        };
    }

    private static boolean isNamed(ClientProperties.File config) {
        return config.dekName() != null && !config.dekName().isBlank();
    }

    private record NamedFileDek(UUID edekId, byte[] dek) {
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
        log.info("file_bulk_named_dek_resolved dek_name={} reused={}", r.key(), r.reused());
        return new NamedFileDek(r.edekId(), dek);
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
        log.info("file_bulk_encrypt_start file_count={}", files.size());

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
                namedDekCache.values().forEach(DekManager::zeroDek);
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
        int filesPerCall = Math.max(1, Math.min(config.filesPerBatch(), svcConfig.dekBatchMaxItems()));
        long sinceFlush = 0;
        for (List<String> batch : partition(slice, filesPerCall)) {
            List<String> toProcess = batch.stream().filter(p -> !alreadyDone.contains(p)).toList();
            if (toProcess.isEmpty()) {
                continue;
            }

            if (namedDek != null) {
                for (String path : toProcess) {
                    encryptOneFile(path, namedDek.edekId(), namedDek.dek());
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
                        encryptOneFile(path, result.edekId(), dek);
                        onFileDone(path, jobId, doneCounter);
                    } finally {
                        DekManager.zeroDek(dek);
                    }
                }
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

    private void encryptOneFile(String relativePath, UUID edekId, byte[] dek) {
        try (InputStream in = sourceStore.openRead(relativePath);
             DataOutputStream out = new DataOutputStream(targetStore.openWrite(relativePath))) {
            out.writeLong(edekId.getMostSignificantBits());
            out.writeLong(edekId.getLeastSignificantBits());

            byte[] buffer = new byte[config.chunkSizeBytes()];
            int read;
            while ((read = readFully(in, buffer)) > 0) {
                byte[] chunk = read == buffer.length ? buffer : Arrays.copyOf(buffer, read);
                DekManager.EncryptResult encrypted = DekManager.encrypt(chunk, dek, svcConfig.appId());
                byte[] frame = new byte[DekManager.IV_LENGTH + DekManager.TAG_LENGTH + encrypted.ciphertext().length];
                System.arraycopy(encrypted.iv(), 0, frame, 0, DekManager.IV_LENGTH);
                System.arraycopy(encrypted.tag(), 0, frame, DekManager.IV_LENGTH, DekManager.TAG_LENGTH);
                System.arraycopy(encrypted.ciphertext(), 0, frame, DekManager.IV_LENGTH + DekManager.TAG_LENGTH, encrypted.ciphertext().length);
                out.writeInt(frame.length);
                out.write(frame);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encrypt file " + relativePath, e);
        }
    }

    private void decryptSlice(List<String> slice, Set<String> alreadyDone, String jobId, AtomicLong doneCounter) {
        int filesPerCall = Math.max(1, Math.min(config.filesPerBatch(), svcConfig.dekBatchMaxItems()));
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

            Map<UUID, byte[]> dekByEdekId = new LinkedHashMap<>();
            if (namedDekCache != null) {
                for (UUID id : distinctEdekIds) {
                    byte[] cached = namedDekCache.get(id);
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
                        dekByEdekId.put(e.getKey(), dek);
                        if (namedDekCache != null) {
                            namedDekCache.put(e.getKey(), dek);
                        }
                    }
                }

                for (String path : toProcess) {
                    UUID edekId = edekIdByPath.get(path);
                    byte[] dek = dekByEdekId.get(edekId);
                    if (dek == null) {
                        SvcClient.UnwrapResult result = resultByEdekId.get(edekId);
                        throw new IllegalStateException("dek/unwrap failed for file " + path
                                + ": " + (result == null ? "no result returned" : result.detail()));
                    }
                    decryptOneFile(path, dek);
                    onFileDone(path, jobId, doneCounter);
                }
            } finally {
                // Only zero DEKs NOT retained in namedDekCache -- those are the same
                // byte[] instances the cache holds for future batches, zeroing them
                // here would corrupt the cache for later reuse.
                for (Map.Entry<UUID, byte[]> e : dekByEdekId.entrySet()) {
                    if (namedDekCache == null || !namedDekCache.containsKey(e.getKey())) {
                        DekManager.zeroDek(e.getValue());
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
        try (DataInputStream in = new DataInputStream(sourceStore.openRead(relativePath))) {
            long msb = in.readLong();
            long lsb = in.readLong();
            return new UUID(msb, lsb);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read edek_id header from " + relativePath, e);
        }
    }

    private void decryptOneFile(String relativePath, byte[] dek) {
        try (DataInputStream in = new DataInputStream(sourceStore.openRead(relativePath));
             OutputStream out = targetStore.openWrite(relativePath)) {
            in.readLong(); // edek_id -- already consumed via readEdekIdHeader before the /dek/unwrap call
            in.readLong();

            while (true) {
                int frameLength;
                try {
                    frameLength = in.readInt();
                } catch (EOFException eof) {
                    break;
                }
                byte[] frame = new byte[frameLength];
                in.readFully(frame);
                byte[] iv = Arrays.copyOfRange(frame, 0, DekManager.IV_LENGTH);
                byte[] tag = Arrays.copyOfRange(frame, DekManager.IV_LENGTH, DekManager.IV_LENGTH + DekManager.TAG_LENGTH);
                byte[] ciphertext = Arrays.copyOfRange(frame, DekManager.IV_LENGTH + DekManager.TAG_LENGTH, frame.length);
                try {
                    byte[] plaintext = DekManager.decrypt(ciphertext, tag, iv, dek, svcConfig.appId());
                    out.write(plaintext);
                } catch (AEADBadTagException e) {
                    throw new IllegalStateException("AEAD tag verification failed decrypting chunk of " + relativePath, e);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to decrypt file " + relativePath, e);
        }
    }

    /** Reads up to buffer.length bytes, filling the buffer as much as possible before returning (unlike InputStream.read, which may return short reads) -- so chunk sizes are consistent except for the final chunk. Returns 0 at EOF. */
    private static int readFully(InputStream in, byte[] buffer) throws IOException {
        int total = 0;
        while (total < buffer.length) {
            int n = in.read(buffer, total, buffer.length - total);
            if (n < 0) {
                break;
            }
            total += n;
        }
        return total;
    }

    private static List<List<String>> partition(List<String> items, int size) {
        List<List<String>> result = new ArrayList<>();
        for (int i = 0; i < items.size(); i += size) {
            result.add(items.subList(i, Math.min(i + size, items.size())));
        }
        return result;
    }
}
