package com.hsm.fileservice.web;

import com.hsm.fileservice.audit.AccessAuditLogger;
import com.hsm.fileservice.config.FileServiceProperties;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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
 * {@code GET <api-prefix>/files/{path}} -- the service's only data endpoint, where
 * api-prefix is this service's own prefix (hsm.file-service.server.api-prefix, default
 * /api/sensec/file/v1). Read-only by design: there is no upload, list or delete. Request headers:
 * <ul>
 *   <li>{@code X-Expected-File-Id} (optional, or required via access.require-expected-file-id):
 *       the file_id the BFF recorded when the file was written; a mismatch is 412.</li>
 *   <li>{@code X-End-User} (optional): who the BFF is serving -- audit only, never authorization.</li>
 *   <li>{@code X-Request-Id} (optional): correlation id, echoed back.</li>
 * </ul>
 */
@RestController
@Tag(name = "Files", description = "Decrypt-and-serve. The service's only data endpoint.")
public class FileController {

    /** Resource segment under the API prefix: the endpoint is {@code <api-prefix>/files/{path}}. */
    static final String FILES = "/files/";
    static final String END_USER_HEADER = "X-End-User";

    private static final Logger log = LoggerFactory.getLogger(FileController.class);

    private final FileDeliveryService delivery;
    private final AccessAuditLogger audit;
    private final MeterRegistry meters;
    private final List<String> allowedPrefixes;
    private final List<String> trustedCallers;
    private final String filesBase;

    public FileController(FileDeliveryService delivery, AccessAuditLogger audit, MeterRegistry meters,
                          FileServiceProperties props) {
        this.delivery = delivery;
        this.audit = audit;
        this.meters = meters;
        this.allowedPrefixes = props.access().allowedPathPrefixes();
        this.trustedCallers = props.access().trustedCallerSpiffeIds();
        this.filesBase = props.server().apiPrefix() + FILES;
        if (allowedPrefixes.isEmpty()) {
            throw new IllegalStateException("hsm.file-service.access.allowed-path-prefixes must list at least one prefix (use \"*\" to allow every path explicitly)");
        }
    }

    @Operation(
            operationId = "getFile",
            summary = "Download one decrypted file",
            description = """
                    Reads the encrypted file at {path} (relative to store.root), verifies every chunk and returns the
                    original bytes. Read-only; only the consumer's BFF may call it (Istio AuthorizationPolicy).

                    Delivery: stored size <= delivery.buffer-threshold-bytes (default 21.5 MiB ~= 16 MiB original) is
                    verified completely before the first byte and sent with Content-Length. Larger files stream with
                    chunked transfer; a verification failure after the first byte ABORTS THE CONNECTION instead of
                    returning an error body -- the caller must treat an incomplete transfer as a failure.

                    Every failure before the first byte is an ErrorResponse. A caller that is not the BFF is refused by
                    the Istio sidecar with a plain-text "403 RBAC: access denied" before reaching the service.""")
    @Parameters({
            // The {path} parameter itself is added by FileServiceOpenApiConfig: the real mapping is
            // <api-prefix>/files/** (multi-segment), which springdoc can't turn into a path variable.
            @Parameter(name = FileDeliveryService.EXPECTED_FILE_ID_HEADER, in = ParameterIn.HEADER,
                    description = "file_id recorded when the file was written (bulk-client result files). Mismatch -> 412. "
                            + "Required when access.require-expected-file-id=true (428 if missing).",
                    schema = @Schema(type = "string", format = "uuid")),
            @Parameter(name = END_USER_HEADER, in = ParameterIn.HEADER,
                    description = "Who the BFF is serving. Recorded in the audit line only, never used for access decisions. Truncated to 256 chars.",
                    schema = @Schema(type = "string", maxLength = 256)),
            @Parameter(name = RequestIdFilter.HEADER, in = ParameterIn.HEADER,
                    description = "Correlation id, echoed back. Replaced by a generated UUID unless it matches the pattern.",
                    schema = @Schema(type = "string", pattern = "^[A-Za-z0-9._:-]{1,128}$"))
    })
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "The original file bytes.",
                    content = @Content(mediaType = "*/*", schema = @Schema(type = "string", format = "binary")),
                    headers = {
                            @Header(name = "Content-Type", description = "From the file extension, else application/octet-stream", schema = @Schema(type = "string")),
                            @Header(name = "Content-Length", description = "Buffered delivery only; absent when streaming (chunked)", schema = @Schema(type = "integer", format = "int64")),
                            @Header(name = "Content-Disposition", description = "inline (default) or attachment, filename = last path segment (RFC 5987)", schema = @Schema(type = "string")),
                            @Header(name = "Cache-Control", description = "Always no-store", schema = @Schema(type = "string")),
                            @Header(name = "X-Content-Type-Options", description = "Always nosniff", schema = @Schema(type = "string")),
                            @Header(name = "X-HSM-Format-Version", description = "1 or 2", schema = @Schema(type = "string", allowableValues = {"1", "2"})),
                            @Header(name = "X-HSM-File-Id", description = "v2 files only", schema = @Schema(type = "string", format = "uuid")),
                            @Header(name = "X-HSM-Delivery", description = "buffered or streaming", schema = @Schema(type = "string", allowableValues = {"buffered", "streaming"})),
                            @Header(name = RequestIdFilter.HEADER, description = "Correlation id", schema = @Schema(type = "string"))
                    }),
            @ApiResponse(responseCode = "400", description = "FS-400-BAD-PATH, FS-400-BAD-FILE-ID", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "403", description = "FS-403-CALLER-NOT-TRUSTED (access.trusted-caller-spiffe-ids)", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "404", description = "FS-404-NOT-FOUND: missing, outside allowed-path-prefixes, or bulk bookkeeping", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "412", description = "FS-412-FILE-ID-MISMATCH, FS-412-NO-FILE-ID", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "422", description = "FS-422-INTEGRITY (tampered/truncated/reordered/spliced), FS-422-LIMIT", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "428", description = "FS-428-FILE-ID-REQUIRED", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "FS-500-INTERNAL", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "502", description = "FS-502-KEY-UNAVAILABLE (grant missing / key shredded), FS-502-STORAGE", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "503", description = "FS-503-CORE-UNAVAILABLE -- safe to retry with backoff", content = @Content(mediaType = "application/json", schema = @Schema(implementation = ErrorResponse.class)))
    })
    @GetMapping("${hsm.file-service.server.api-prefix:" + FileServiceProperties.Server.DEFAULT_API_PREFIX + "}" + FILES + "**")
    public void get(@Parameter(hidden = true) HttpServletRequest request, @Parameter(hidden = true) HttpServletResponse response) {
        long start = System.nanoTime();
        RequestTrace trace = new RequestTrace();
        trace.endUser = truncate(request.getHeader(END_USER_HEADER));
        trace.caller = CallerIdentity.immediatePeerSpiffeId(request.getHeader(CallerIdentity.XFCC_HEADER));
        try {
            if (!trustedCallers.isEmpty() && (trace.caller == null || !trustedCallers.contains(trace.caller))) {
                throw new FileServiceException(ErrorCode.CALLER_NOT_TRUSTED, "peer " + trace.caller + " not in trusted-caller-spiffe-ids");
            }
            String path = RequestPaths.validate(extractPath(request, filesBase));
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

    /** Path after {@code <api-prefix>/files/}, percent-decoded as a URI path (not as a form: "+" stays "+"). */
    static String extractPath(HttpServletRequest request, String filesBase) {
        String uri = request.getRequestURI();
        String prefix = request.getContextPath() + filesBase;
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
