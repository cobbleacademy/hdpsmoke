package com.hsm.fileservice.web;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * JSON error body for every failure that happens before the first byte of a file is
 * sent: {@code {"error_code": "...", "message": "...", "request_id": "..."}}. Fixed
 * messages only (see ErrorCode). StreamAbortedException is intentionally not handled
 * here -- see its javadoc.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(FileServiceException.class)
    public ResponseEntity<Map<String, String>> handle(FileServiceException e, HttpServletRequest request) {
        Map<String, String> body = new LinkedHashMap<>();
        body.put("error_code", e.code().code());
        body.put("message", e.code().message());
        String requestId = RequestIdFilter.of(request);
        body.put("request_id", requestId);
        ResponseEntity.BodyBuilder builder = ResponseEntity.status(e.code().status())
                .contentType(MediaType.APPLICATION_JSON)
                .header(HttpHeaders.CACHE_CONTROL, "no-store");
        if (requestId != null) {
            // Re-set: a streaming failure before the first byte resets the response, clearing it.
            builder.header(RequestIdFilter.HEADER, requestId);
        }
        return builder.body(body);
    }
}
