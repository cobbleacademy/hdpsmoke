package com.hsm.fileservice.web;

/**
 * Thrown when a streamed download fails after bytes have already been sent. Deliberately
 * NOT handled by any @ExceptionHandler: it propagates to Tomcat, which -- because the
 * response is already committed -- closes the connection instead of finishing the
 * chunked body. The client therefore sees an incomplete transfer (an error), never a
 * normal end-of-response on partial content. The BFF must treat that as a failure.
 */
public class StreamAbortedException extends RuntimeException {

    private final ErrorCode code;

    public StreamAbortedException(ErrorCode code, String detail, Throwable cause) {
        super(detail, cause);
        this.code = code;
    }

    public ErrorCode code() {
        return code;
    }
}
