package com.hsm.core.lock;

import com.hsm.core.web.ApiException;
import org.springframework.http.HttpStatus;

/**
 * The {@link JobLock} for this operation is held by another run (this pod or
 * any other sharing the database). An ApiException so the admin endpoints
 * answer 409 through GlobalExceptionHandler unchanged; the schedulers catch it
 * and log a skip instead of an error.
 */
public class JobAlreadyRunningException extends ApiException {

    public JobAlreadyRunningException(String message) {
        super(HttpStatus.CONFLICT, message);
    }
}
