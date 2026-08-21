package com.hsm.client.file;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Storage abstraction so FileBulkJob's chunking/framing/DEK-per-file logic is
 * entirely independent of where bytes come from or go to. Source and target are
 * each independently one of {@link LocalFileStore}, {@link AdlsFileStore}
 * (ADLS Gen2, requires Hierarchical Namespace), or {@link AzureBlobFileStore}
 * (plain Azure Blob Storage, no HNS required) -- the job logic never checks
 * which, it just calls list()/openRead() on one instance and openWrite() on
 * another. Mixed pairs (e.g. ADLS source -> local target) fall out of this
 * for free.
 *
 * <p>All paths are relative to whatever root the FileStore was constructed with, so
 * FileBulkJob can mirror a relative path (e.g. "level1/level2/sensitive.png")
 * unchanged from source to target regardless of which store implementation is on
 * either side.
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

    /** Recursively list every file under the store's root whose name ends with one of fileTypes (case-insensitive), or all files if fileTypes is empty -- always excluding MANIFEST_DIR. Returns paths relative to root. */
    List<String> list(List<String> fileTypes);

    InputStream openRead(String relativePath);

    /** Opens (creating parent directories/paths as needed) a stream to write relativePath under the store's root. */
    OutputStream openWrite(String relativePath);
}
