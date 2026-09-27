package com.hsm.filestore;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Storage abstraction so the chunking/framing logic (hsm-crypto-client's
 * EncryptedFileWriter/Reader) and its callers -- hsm-bulk-client's FileBulkJob and
 * hsm-file-service -- are entirely independent of where bytes come from or go to.
 * Source and target are each independently one of {@link LocalFileStore},
 * {@link AdlsFileStore} (ADLS Gen2, requires Hierarchical Namespace), or
 * {@link AzureBlobFileStore} (plain Azure Blob Storage, no HNS required) -- callers
 * never check which, they just call list()/openRead()/size() on one instance and
 * openWrite() on another. Mixed pairs (e.g. ADLS source -> local target) fall out of
 * this for free.
 *
 * <p>Lives in its own module (not hsm-crypto-client) so the crypto library stays free
 * of the Azure Storage SDKs -- embedders such as hsm-spark-adapter never pull them in.
 *
 * <p>All paths are relative to whatever root the FileStore was constructed with, so a
 * relative path (e.g. "level1/level2/sensitive.png") mirrors unchanged from source to
 * target regardless of which store implementation is on either side.
 */
public interface FileStore {

    /**
     * Directory FileCheckpointStore writes its manifest under, at the target store's
     * root -- list() implementations must never return paths under this directory,
     * or a checkpoint-enabled job whose source is a prior job's target (the common
     * encrypt-then-decrypt pattern) would try to process its own manifest file as
     * if it were job data: on decrypt, the manifest's plain-text bytes get read as a
     * fabricated edek_id header, which then fails /dek/unwrap with "EDEK not found"
     * since that id was never actually issued.
     */
    String MANIFEST_DIR = ".hsm_bulk_checkpoint";

    /** Directory FileBulkJob writes its per-batch result files (path -> file_id) under; excluded from list() for the same reason as MANIFEST_DIR. */
    String RESULTS_DIR = ".hsm_bulk_results";

    /** Recursively list every file under the store's root whose name ends with one of fileTypes (case-insensitive), or all files if fileTypes is empty -- always excluding MANIFEST_DIR and RESULTS_DIR. Returns paths relative to root. */
    List<String> list(List<String> fileTypes);

    InputStream openRead(String relativePath);

    /** Opens (creating parent directories/paths as needed) a stream to write relativePath under the store's root. */
    OutputStream openWrite(String relativePath);

    /**
     * Stored size in bytes, without reading the content.
     *
     * @throws StoreFileNotFoundException if nothing is stored at relativePath
     */
    long size(String relativePath);

    /** True for paths list() must skip: the bookkeeping directories above. */
    static boolean isInternalPath(String relativePath) {
        return relativePath.startsWith(MANIFEST_DIR + "/") || relativePath.startsWith(RESULTS_DIR + "/");
    }
}
