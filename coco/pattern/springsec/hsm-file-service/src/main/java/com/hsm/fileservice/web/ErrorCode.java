package com.hsm.fileservice.web;

import org.springframework.http.HttpStatus;

/**
 * Stable, documented error codes (java/docs/FILE_SERVICE.md "Error codes"). Consumers
 * operate this service without its source, so each code maps to one runbook entry;
 * codes are never renumbered, only added. Response bodies carry the code and a fixed
 * message -- never paths, ids, key material or upstream error text; the detail goes to
 * the service's own logs, keyed by request_id.
 */
public enum ErrorCode {
    BAD_PATH("FS-400-BAD-PATH", HttpStatus.BAD_REQUEST, "Invalid file path"),
    BAD_FILE_ID("FS-400-BAD-FILE-ID", HttpStatus.BAD_REQUEST, "X-Expected-File-Id is not a valid UUID"),
    CALLER_NOT_TRUSTED("FS-403-CALLER-NOT-TRUSTED", HttpStatus.FORBIDDEN, "Caller is not allowed"),
    NOT_FOUND("FS-404-NOT-FOUND", HttpStatus.NOT_FOUND, "File not found"),
    FILE_ID_MISMATCH("FS-412-FILE-ID-MISMATCH", HttpStatus.PRECONDITION_FAILED, "Stored file does not have the expected file id"),
    NO_FILE_ID("FS-412-NO-FILE-ID", HttpStatus.PRECONDITION_FAILED, "Stored file is format v1 and carries no file id"),
    FILE_ID_REQUIRED("FS-428-FILE-ID-REQUIRED", HttpStatus.PRECONDITION_REQUIRED, "X-Expected-File-Id is required"),
    INTEGRITY("FS-422-INTEGRITY", HttpStatus.UNPROCESSABLE_CONTENT, "Stored file failed integrity verification"),
    LIMIT("FS-422-LIMIT", HttpStatus.UNPROCESSABLE_CONTENT, "Stored file exceeds configured limits"),
    KEY_UNAVAILABLE("FS-502-KEY-UNAVAILABLE", HttpStatus.BAD_GATEWAY, "Decryption key unavailable"),
    STORAGE("FS-502-STORAGE", HttpStatus.BAD_GATEWAY, "Storage read failed"),
    CORE_UNAVAILABLE("FS-503-CORE-UNAVAILABLE", HttpStatus.SERVICE_UNAVAILABLE, "Key service unavailable"),
    INTERNAL("FS-500-INTERNAL", HttpStatus.INTERNAL_SERVER_ERROR, "Internal error");

    private final String code;
    private final HttpStatus status;
    private final String message;

    ErrorCode(String code, HttpStatus status, String message) {
        this.code = code;
        this.status = status;
        this.message = message;
    }

    public String code() {
        return code;
    }

    public HttpStatus status() {
        return status;
    }

    public String message() {
        return message;
    }
}
