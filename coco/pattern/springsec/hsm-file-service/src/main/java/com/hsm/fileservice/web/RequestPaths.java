package com.hsm.fileservice.web;

import java.util.List;

/**
 * Validates the caller-supplied file path before it reaches any FileStore. Rejects
 * rather than normalizes: a path that needs normalizing is never a legitimate request.
 */
public final class RequestPaths {

    static final int MAX_LENGTH = 1024;

    private RequestPaths() {
    }

    /** Returns the path unchanged if acceptable, else throws BAD_PATH. */
    public static String validate(String path) {
        if (path == null || path.isEmpty() || path.length() > MAX_LENGTH) {
            throw new FileServiceException(ErrorCode.BAD_PATH, "empty or over-long path");
        }
        if (path.startsWith("/") || path.endsWith("/") || path.contains("//") || path.contains("\\")) {
            throw new FileServiceException(ErrorCode.BAD_PATH, "path has leading/trailing/empty segments or backslashes");
        }
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c < 0x20 || c == 0x7f) {
                throw new FileServiceException(ErrorCode.BAD_PATH, "path contains control characters");
            }
        }
        for (String segment : path.split("/")) {
            if (segment.equals(".") || segment.equals("..")) {
                throw new FileServiceException(ErrorCode.BAD_PATH, "path contains dot segments");
            }
        }
        if (path.startsWith(".hsm_bulk_")) {
            // Bulk-job bookkeeping (checkpoint manifests, result files) is never served.
            throw new FileServiceException(ErrorCode.NOT_FOUND, "internal bookkeeping path");
        }
        return path;
    }

    /** Prefix match at a segment boundary: "tenant-a" allows "tenant-a/x" but not "tenant-ab/x". "*" allows everything. */
    public static boolean isAllowed(String path, List<String> allowedPrefixes) {
        for (String raw : allowedPrefixes) {
            String prefix = raw.strip();
            if (prefix.equals("*")) {
                return true;
            }
            String dir = prefix.endsWith("/") ? prefix : prefix + "/";
            if (path.startsWith(dir) || path.equals(prefix)) {
                return true;
            }
        }
        return false;
    }
}
