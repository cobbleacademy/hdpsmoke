package com.hsm.fileservice.web;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

/**
 * The only JSON body this service ever returns: every failure before the first byte
 * of a file. One type for both the handler and the OpenAPI spec, so the documented
 * shape can't drift from the real one. The allowed error_code values are filled in
 * from {@link ErrorCode} by FileServiceOpenApiConfig.
 */
@Schema(name = "ErrorResponse", description = "Failure before any file bytes were sent. message is fixed per code; details are in the service log under request_id.")
public record ErrorResponse(
        @JsonProperty("error_code") @Schema(requiredMode = Schema.RequiredMode.REQUIRED, example = "FS-412-FILE-ID-MISMATCH")
        String errorCode,
        @Schema(requiredMode = Schema.RequiredMode.REQUIRED, example = "Stored file does not have the expected file id")
        String message,
        @JsonProperty("request_id") @Schema(requiredMode = Schema.RequiredMode.REQUIRED, example = "req-7f3a91")
        String requestId) {
}
