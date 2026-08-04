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
import java.util.UUID;

/**
 * BULK File job: one DEK per whole file, each chunk encrypted separately under that
 * DEK (DekManager.encrypt draws a fresh random IV per call, so this needs no new
 * crypto design). Chunks are stitched into a single output file via length-prefixed
 * binary framing -- immune to ciphertext content, unlike a newline delimiter.
 *
 * <p>Output file layout: [16 bytes edek_id] then repeated [4-byte chunk length]
 * [iv(12) + tag(16) + ciphertext(N)] until EOF. No explicit chunk-count field (a
 * simplification from the original per-file header sketch): the framing is
 * self-terminating at end-of-stream, which avoids needing to know the chunk count
 * up front -- important for FileStore.openWrite's push-style streaming (in
 * particular AdlsFileStore's pipe-based upload), which can't seek back to fill in a
 * count after the fact.
 */
public class FileBulkJob {

    private static final Logger log = LoggerFactory.getLogger(FileBulkJob.class);

    private final ClientProperties.File config;
    private final ClientProperties.Svc svcConfig;
    private final SvcClient svcClient;
    private final PrivateKey privateKey;
    private final FileStore sourceStore;
    private final FileStore targetStore;

    public FileBulkJob(ClientProperties.File config, ClientProperties.Svc svcConfig, SvcClient svcClient) {
        this.config = config;
        this.svcConfig = svcConfig;
        this.svcClient = svcClient;
        this.privateKey = TransportWrapper.parsePrivateKeyPem(svcConfig.privateKeyPem());
        this.sourceStore = buildStore(config.source());
        this.targetStore = buildStore(config.target());
    }

    private static FileStore buildStore(ClientProperties.File.StoreRef ref) {
        return switch (ref.type()) {
            case LOCAL -> new LocalFileStore(ref.root());
            case ADLS -> new AdlsFileStore(ref.root());
        };
    }

    public void encrypt() {
        List<String> files = sourceStore.list(config.fileTypes());
        log.info("file_bulk_encrypt_start file_count={}", files.size());

        int filesPerCall = Math.max(1, Math.min(config.filesPerBatch(), svcConfig.dekBatchMaxItems()));
        int done = 0;
        for (List<String> batch : partition(files, filesPerCall)) {
            List<SvcClient.IssueItem> issueItems = batch.stream()
                    .map(path -> new SvcClient.IssueItem(path, null, null))
                    .toList();
            List<SvcClient.IssueResult> issued = svcClient.issue(issueItems);
            Map<String, SvcClient.IssueResult> byKey = new LinkedHashMap<>();
            for (SvcClient.IssueResult r : issued) {
                byKey.put(r.key(), r);
            }

            for (String path : batch) {
                SvcClient.IssueResult result = byKey.get(path);
                if (result == null || !"success".equals(result.status())) {
                    throw new IllegalStateException("dek/issue failed for file " + path
                            + ": " + (result == null ? "no result returned" : result.detail()));
                }
                byte[] dek = TransportWrapper.unwrap(Base64.getDecoder().decode(result.wrappedDekB64()), privateKey);
                try {
                    encryptOneFile(path, result.edekId(), dek);
                } finally {
                    DekManager.zeroDek(dek);
                }
                done++;
            }
            log.info("file_bulk_encrypt_progress files_done={}", done);
        }
        log.info("file_bulk_encrypt_complete total_files={}", done);
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

    public void decrypt() {
        List<String> files = sourceStore.list(config.fileTypes());
        log.info("file_bulk_decrypt_start file_count={}", files.size());

        int filesPerCall = Math.max(1, Math.min(config.filesPerBatch(), svcConfig.dekBatchMaxItems()));
        int done = 0;
        for (List<String> batch : partition(files, filesPerCall)) {
            List<SvcClient.UnwrapItem> unwrapItems = new ArrayList<>();
            for (String path : batch) {
                unwrapItems.add(new SvcClient.UnwrapItem(path, readEdekIdHeader(path)));
            }
            List<SvcClient.UnwrapResult> unwrapped = svcClient.unwrap(unwrapItems);
            Map<String, SvcClient.UnwrapResult> byKey = new LinkedHashMap<>();
            for (SvcClient.UnwrapResult r : unwrapped) {
                byKey.put(r.key(), r);
            }

            for (String path : batch) {
                SvcClient.UnwrapResult result = byKey.get(path);
                if (result == null || !"success".equals(result.status())) {
                    throw new IllegalStateException("dek/unwrap failed for file " + path
                            + ": " + (result == null ? "no result returned" : result.detail()));
                }
                byte[] dek = TransportWrapper.unwrap(Base64.getDecoder().decode(result.wrappedDekB64()), privateKey);
                try {
                    decryptOneFile(path, dek);
                } finally {
                    DekManager.zeroDek(dek);
                }
                done++;
            }
            log.info("file_bulk_decrypt_progress files_done={}", done);
        }
        log.info("file_bulk_decrypt_complete total_files={}", done);
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
