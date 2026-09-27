package com.hsm.filestore;

/** Uniform "nothing stored at this path" across Local/ADLS/Blob, so callers can map it to a 404 without knowing the backend. */
public class StoreFileNotFoundException extends RuntimeException {

    public StoreFileNotFoundException(String relativePath, Throwable cause) {
        super("No file stored at " + relativePath, cause);
    }
}
