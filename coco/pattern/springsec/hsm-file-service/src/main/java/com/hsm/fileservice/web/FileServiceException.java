package com.hsm.fileservice.web;

/** A request failure with a stable code. {@code detail} is for logs only and never reaches the response body. */
public class FileServiceException extends RuntimeException {

    private final ErrorCode code;

    public FileServiceException(ErrorCode code, String detail) {
        super(detail);
        this.code = code;
    }

    public FileServiceException(ErrorCode code, String detail, Throwable cause) {
        super(detail, cause);
        this.code = code;
    }

    public ErrorCode code() {
        return code;
    }
}
