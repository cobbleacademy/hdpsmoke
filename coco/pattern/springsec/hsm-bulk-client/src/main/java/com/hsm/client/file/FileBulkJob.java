package com.hsm.client.file;

import com.hsm.client.config.ClientProperties;
import com.hsm.client.crypto.DekManager;
import com.hsm.client.crypto.TransportWrapper;
import com.hsm.client.svc.SvcClient;
import com.hsm.client.svc.SvcConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import javax.crypto.AEADBadTagException;
import java.nio.charset.StandardCharsets;
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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

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
 * count after the fact. edek_id is stored once, not repeated per chunk -- every
 * chunk of one file always shares the same DEK by construction, so nothing is lost
 * by only writing it once; dek_name is deliberately NOT persisted here at all, since
 * it has no role at decrypt time on either service (it only ever affects encrypt-time
 * DEK-reuse decisions) -- edek_id is the one thing hsm-bulk-service's /dek/unwrap and
 * hsm-core-service's /decrypt both actually key off of.
 *
 * <p>Each chunk's plaintext is base64-encoded (config.chunkSizeBytes() raw bytes in,
 * an ASCII base64 string out) before it's actually encrypted -- required, not
 * cosmetic: hsm-core-service's own DecryptionService does {@code new String(plaintext,
 * UTF_8)} unconditionally on the way out, which corrupts arbitrary binary content (a
 * real file chunk is essentially never valid UTF-8) but is always lossless for base64
 * text, since base64's alphabet is a strict subset of ASCII/UTF-8. This is the one
 * change needed to make this file's own ciphertext frames decryptable via
 * hsm-core-service, not just this class's own local decrypt path.
 *
 * <p>config.compressBeforeEncrypt() (default false, per-job only -- see
 * ClientProperties.File's javadoc) gzips each chunk before the base64 step above.
 * The one byte immediately BEFORE the base64-encoded payload -- itself inside the
 * AES-GCM-protected plaintext, so it's authenticated, not just self-describing --
 * is a marker: {@code 0x00} raw, {@code 0x01} gzip-compressed. Every decrypt path
 * (this class's own decryptOneFile, and the remote path via
 * reconstructCoreServiceToken) always reads this marker and branches accordingly,
 * regardless of what compressBeforeEncrypt was set to on whatever job produced the
 * file, or which service resolves the DEK -- no coordination needed between the
 * encrypt-time config and whatever decrypts later.
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
    private final Map<UUID, byte[]> namedDekCache;

    public FileBulkJob(ClientProperties.File config, SvcConfig svcConfig, SvcClient svcClient) {
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
            case ADLS -> new AdlsFileStore(ref.root(), ref.accountKey());
            case AZURE_BLOB -> new AzureBlobFileStore(ref.root(), ref.accountKey());
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

            boolean compress = config.compressBeforeEncrypt();
            byte[] buffer = new byte[config.chunkSizeBytes()];
            int read;
            while ((read = readFully(in, buffer)) > 0) {
                byte[] chunk = read == buffer.length ? buffer : Arrays.copyOf(buffer, read);
                // Compression marker (see class javadoc) prepended BEFORE base64 --
                // inside the AES-GCM-protected plaintext, so it's authenticated, not
                // just self-describing.
                byte[] payload = compress ? gzip(chunk) : chunk;
                byte[] marked = new byte[1 + payload.length];
                marked[0] = compress ? (byte) 0x01 : (byte) 0x00;
                System.arraycopy(payload, 0, marked, 1, payload.length);
                // base64-encode before encrypting -- see class javadoc: makes this
                // frame's ciphertext safe to decrypt via hsm-core-service's own
                // /decrypt too, not just this class's own local path.
                String base64Plaintext = Base64.getEncoder().encodeToString(marked);
                DekManager.EncryptResult encrypted = DekManager.encrypt(
                        base64Plaintext.getBytes(StandardCharsets.UTF_8), dek, svcConfig.appId());
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
                byte[] plaintext;
                try {
                    plaintext = DekManager.decrypt(ciphertext, tag, iv, dek, svcConfig.appId());
                } catch (AEADBadTagException e) {
                    throw new IllegalStateException("AEAD tag verification failed decrypting chunk of " + relativePath, e);
                }
                // Reverse of encryptOneFile's base64-safety encoding -- plaintext
                // here is the base64 string's UTF-8 bytes, not the raw chunk yet.
                String base64Plaintext = new String(plaintext, StandardCharsets.UTF_8);
                byte[] marked = Base64.getDecoder().decode(base64Plaintext);
                // Marker byte (see class javadoc) always read regardless of this
                // job's own compressBeforeEncrypt config -- self-describing per chunk.
                byte flag = marked[0];
                byte[] payload = Arrays.copyOfRange(marked, 1, marked.length);
                out.write(flag == 0x01 ? gunzip(payload) : payload);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to decrypt file " + relativePath, e);
        }
    }

    /** config.compressBeforeEncrypt() support -- see class javadoc for the marker-byte scheme this feeds. */
    private static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
            gzip.write(data);
        }
        return compressed.toByteArray();
    }

    private static byte[] gunzip(byte[] data) throws IOException {
        ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
        try (GZIPInputStream gunzip = new GZIPInputStream(new ByteArrayInputStream(data))) {
            gunzip.transferTo(decompressed);
        }
        return decompressed.toByteArray();
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
     * directly instead, without ever talking to hsm-bulk-service.
     */
    public static String reconstructCoreServiceToken(UUID edekId, byte[] iv, byte[] tag, byte[] ciphertext) {
        return DekManager.packToken(edekId, iv, tag, ciphertext);
    }
}
