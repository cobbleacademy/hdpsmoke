package com.hsm.client.file;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Storage abstraction so FileBulkJob's chunking/framing/DEK-per-file logic is
 * entirely independent of where bytes come from or go to. Source and target are
 * each independently one of {@link LocalFileStore} or {@link AdlsFileStore} -- the
 * job logic never checks which, it just calls list()/openRead() on one instance and
 * openWrite() on another. Mixed pairs (ADLS source -> local target, or vice versa)
 * fall out of this for free.
 *
 * <p>All paths are relative to whatever root the FileStore was constructed with, so
 * FileBulkJob can mirror a relative path (e.g. "level1/level2/sensitive.png")
 * unchanged from source to target regardless of which store implementation is on
 * either side.
 */
public interface FileStore {

    /** Recursively list every file under the store's root whose name ends with one of fileTypes (case-insensitive), or all files if fileTypes is empty. Returns paths relative to root. */
    List<String> list(List<String> fileTypes);

    InputStream openRead(String relativePath);

    /** Opens (creating parent directories/paths as needed) a stream to write relativePath under the store's root. */
    OutputStream openWrite(String relativePath);
}
