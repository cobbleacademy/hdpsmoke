package com.hsm.core.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.NoHandlerFoundException;
import org.springframework.web.servlet.resource.NoResourceFoundException;
import tools.jackson.databind.exc.UnrecognizedPropertyException;

import java.util.Map;

/** Ported from app/main.py's catch-all exception_handler plus the various HTTPException call sites. */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<Map<String, String>> handleApiException(ApiException e) {
        return ResponseEntity.status(e.getStatus()).body(Map.of("detail", e.getMessage()));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, String>> handleValidation(MethodArgumentNotValidException e) {
        String detail = e.getBindingResult().getFieldErrors().stream()
                .findFirst()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .orElse("Validation failed");
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_CONTENT).body(Map.of("detail", detail));
    }

    /**
     * Body that could not be read: malformed JSON, a wrong value type, or (v2 only --
     * ApiVersionJsonConfig) a field the endpoint does not know. Previously the catch-all
     * below answered all of these with a 500. The unknown-field message names the field,
     * since on v2 that is almost always a snake_case name sent to the camelCase API.
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, String>> handleUnreadableBody(HttpMessageNotReadableException e) {
        String detail = e.getMostSpecificCause() instanceof UnrecognizedPropertyException unknown
                ? "Unknown field '" + unknown.getPropertyName() + "'"
                : "Malformed request body";
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("detail", detail));
    }

    /**
     * Unknown path: without this the catch-all below turned every mistyped URL into a 500 (and an
     * ERROR log line with a stack trace). Spring raises NoResourceFoundException when a static
     * resource handler was tried (the demo UI's, under the API prefix) and NoHandlerFoundException
     * when nothing matched at all (every path outside it, and all paths when demo mode is off).
     */
    @ExceptionHandler({NoResourceFoundException.class, NoHandlerFoundException.class})
    public ResponseEntity<Map<String, String>> handleNotFound(Exception e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("detail", "Not found"));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, String>> handleUnexpected(Exception e, WebRequest request) {
        log.error("unhandled_exception path={} error={}", request.getDescription(false), e.getMessage(), e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("detail", "Internal server error"));
    }
}
