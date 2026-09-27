package com.hsm.fileservice.web;

import com.hsm.fileservice.audit.AccessAuditLogger;
import com.hsm.fileservice.config.FileServiceProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * {@code GET /v1/files/{path}} -- the service's only data endpoint. Read-only by
 * design: there is no upload, list or delete. Request headers:
 * <ul>
 *   <li>{@code X-Expected-File-Id} (optional, or required via access.require-expected-file-id):
 *       the file_id the BFF recorded when the file was written; a mismatch is 412.</li>
 *   <li>{@code X-End-User} (optional): who the BFF is serving -- audit only, never authorization.</li>
 *   <li>{@code X-Request-Id} (optional): correlation id, echoed back.</li>
 * </ul>
 */
@RestController
public class FileController {

    static final String BASE = "/v1/files/";
    static final String END_USER_HEADER = "X-End-User";

    private static final Logger log = LoggerFactory.getLogger(FileController.class);

    private final FileDeliveryService delivery;
    private final AccessAuditLogger audit;
    private final MeterRegistry meters;
    private final List<String> allowedPrefixes;
    private final List<String> trustedCallers;

    public FileController(FileDeliveryService delivery, AccessAuditLogger audit, MeterRegistry meters,
                          FileServiceProperties props) {
        this.delivery = delivery;
        this.audit = audit;
        this.meters = meters;
        this.allowedPrefixes = props.access().allowedPathPrefixes();
        this.trustedCallers = props.access().trustedCallerSpiffeIds();
        if (allowedPrefixes.isEmpty()) {
            throw new IllegalStateException("hsm.file-service.access.allowed-path-prefixes must list at least one prefix (use \"*\" to allow every path explicitly)");
        }
    }

    @GetMapping(BASE + "**")
    public void get(HttpServletRequest request, HttpServletResponse response) {
        long start = System.nanoTime();
        RequestTrace trace = new RequestTrace();
        trace.endUser = truncate(request.getHeader(END_USER_HEADER));
        trace.caller = CallerIdentity.immediatePeerSpiffeId(request.getHeader(CallerIdentity.XFCC_HEADER));
        try {
            if (!trustedCallers.isEmpty() && (trace.caller == null || !trustedCallers.contains(trace.caller))) {
                throw new FileServiceException(ErrorCode.CALLER_NOT_TRUSTED, "peer " + trace.caller + " not in trusted-caller-spiffe-ids");
            }
            String path = RequestPaths.validate(extractPath(request));
            trace.path = path;
            if (!RequestPaths.isAllowed(path, allowedPrefixes)) {
                throw new FileServiceException(ErrorCode.NOT_FOUND, "path outside allowed-path-prefixes");
            }
            delivery.serve(path, request.getHeader(FileDeliveryService.EXPECTED_FILE_ID_HEADER), response, trace);
            trace.outcome = "ok";
        } catch (StreamAbortedException e) {
            if (!"client_closed".equals(trace.outcome)) {
                trace.outcome = "aborted";
                trace.errorCode = e.code() == null ? null : e.code().code();
                log.error("file_stream_aborted request_id={} code={} detail={}", RequestIdFilter.of(request), trace.errorCode, e.getMessage());
            }
            throw e;
        } catch (FileServiceException e) {
            trace.errorCode = e.code().code();
            logFailure(request, e);
            throw e;
        } catch (RuntimeException e) {
            FileServiceException wrapped = new FileServiceException(ErrorCode.INTERNAL, e.toString(), e);
            trace.errorCode = wrapped.code().code();
            logFailure(request, wrapped);
            if (response.isCommitted()) {
                trace.outcome = "aborted";
                throw new StreamAbortedException(ErrorCode.INTERNAL, e.toString(), e);
            }
            throw wrapped;
        } finally {
            long durationNanos = System.nanoTime() - start;
            record(trace, durationNanos);
            audit.log(new AccessAuditLogger.Event(RequestIdFilter.of(request), trace.outcome, trace.errorCode, trace.mode,
                    trace.path, trace.endUser, trace.caller, trace.fileId, trace.formatVersion, trace.bytes,
                    TimeUnit.NANOSECONDS.toMillis(durationNanos)));
        }
    }

    private void logFailure(HttpServletRequest request, FileServiceException e) {
        // Integrity and key failures are security-relevant: ERROR, with the library detail. Client mistakes: WARN.
        boolean serious = e.code().status().is5xxServerError() || e.code() == ErrorCode.INTEGRITY || e.code() == ErrorCode.LIMIT;
        if (serious) {
            log.error("file_request_failed request_id={} code={} detail={}", RequestIdFilter.of(request), e.code().code(), e.getMessage());
        } else {
            log.warn("file_request_rejected request_id={} code={} detail={}", RequestIdFilter.of(request), e.code().code(), e.getMessage());
        }
    }

    private void record(RequestTrace trace, long durationNanos) {
        String code = trace.errorCode == null ? "none" : trace.errorCode;
        meters.counter("hsm.file.requests", "outcome", trace.outcome, "code", code,
                "mode", trace.mode, "format", trace.formatVersion).increment();
        Timer.builder("hsm.file.request.duration").tag("outcome", trace.outcome).tag("mode", trace.mode)
                .register(meters).record(durationNanos, TimeUnit.NANOSECONDS);
        if (trace.bytes > 0) {
            meters.summary("hsm.file.bytes.served", "mode", trace.mode).record(trace.bytes);
        }
    }

    /** Path after /v1/files/, percent-decoded as a URI path (not as a form: "+" stays "+"). */
    static String extractPath(HttpServletRequest request) {
        String uri = request.getRequestURI();
        String prefix = request.getContextPath() + BASE;
        if (!uri.startsWith(prefix)) {
            throw new FileServiceException(ErrorCode.BAD_PATH, "unexpected request URI");
        }
        try {
            return UriUtils.decode(uri.substring(prefix.length()), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new FileServiceException(ErrorCode.BAD_PATH, "invalid percent-encoding");
        }
    }

    private static String truncate(String s) {
        return s == null || s.length() <= 256 ? s : s.substring(0, 256);
    }
}
