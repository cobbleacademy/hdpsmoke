package com.hsm.fileservice.web;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * {@link ErrorResponse} as API v2 sends it: the same three values, camelCase field names
 * ({@code errorCode}, {@code requestId}). Returned for requests under
 * hsm.file-service.server.api-v2-prefix; v1 keeps the snake_case ErrorResponse.
 * The v2 OpenAPI document derives its schema from ErrorResponse (FileServiceOpenApiConfig),
 * so this type is not referenced from annotations.
 */
@Schema(hidden = true)
public record ErrorResponseV2(String errorCode, String message, String requestId) {
}
