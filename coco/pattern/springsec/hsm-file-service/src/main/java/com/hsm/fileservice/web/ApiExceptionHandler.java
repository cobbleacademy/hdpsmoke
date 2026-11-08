package com.hsm.fileservice.web;

import com.hsm.chassis.web.RequestIdFilter;
import com.hsm.fileservice.config.FileServiceProperties;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * JSON error body for every failure that happens before the first byte of a file is
 * sent -- see {@link ErrorResponse}. Fixed messages only (see ErrorCode).
 * StreamAbortedException is intentionally not handled here -- see its javadoc.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private final FileServiceProperties.Server server;

    public ApiExceptionHandler(FileServiceProperties props) {
        this.server = props.server();
    }

    @ExceptionHandler(FileServiceException.class)
    public ResponseEntity<Object> handle(FileServiceException e, HttpServletRequest request) {
        String requestId = RequestIdFilter.of(request);
        ResponseEntity.BodyBuilder builder = ResponseEntity.status(e.code().status())
                .contentType(MediaType.APPLICATION_JSON)
                .header(HttpHeaders.CACHE_CONTROL, "no-store");
        if (requestId != null) {
            // Re-set: a streaming failure before the first byte resets the response, clearing it.
            builder.header(RequestIdFilter.DEFAULT_HEADER, requestId);
        }
        return builder.body(server.isV2(request.getRequestURI())
                ? new ErrorResponseV2(e.code().code(), e.code().message(), requestId)
                : new ErrorResponse(e.code().code(), e.code().message(), requestId));
    }
}
