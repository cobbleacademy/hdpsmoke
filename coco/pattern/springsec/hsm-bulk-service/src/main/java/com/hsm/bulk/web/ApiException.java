package com.hsm.bulk.web;

import org.springframework.http.HttpStatus;

/** Duplicated from com.hsm.core.web.ApiException -- a status code plus a "detail" message, caught by GlobalExceptionHandler. */
public class ApiException extends RuntimeException {

    private final HttpStatus status;

    public ApiException(HttpStatus status, String message) {
        super(message);
        this.status = status;
    }

    public HttpStatus getStatus() {
        return status;
    }
}
