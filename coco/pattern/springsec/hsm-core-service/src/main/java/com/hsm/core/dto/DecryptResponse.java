package com.hsm.core.dto;

import com.fasterxml.jackson.annotation.JsonView;
import com.hsm.core.web.ResponseViews;

/**
 * Ported from app/models/schemas.py's DecryptResponse. Field visibility split
 * via {@code @JsonView} -- see EncryptResponse's javadoc for the full
 * rationale, including why every field needs an explicit view (Jackson 3's
 * MapperFeature.DEFAULT_VIEW_INCLUSION defaults to false, unlike Jackson 2).
 * encoding stays in the minimal view (unlike EncryptResponse's informational
 * fields) since it's functionally necessary: the caller needs it to
 * correctly interpret plaintext (utf8 vs base64), not just informational.
 */
public record DecryptResponse(
        @JsonView(ResponseViews.Minimal.class) String plaintext,
        @JsonView(ResponseViews.Full.class) String ownerAppId, // the app_id used as AAD when this record was encrypted
        @JsonView(ResponseViews.Full.class) String algorithm,
        @JsonView(ResponseViews.Minimal.class) String encoding,   // tells the caller how to interpret plaintext (utf8 vs base64) -- functional, not just informational

        // Additive response envelope (requirement 6) -- all existing fields above are
        // unchanged, nothing renamed. Error responses go through GlobalExceptionHandler's
        // separate {"detail": "..."} shape untouched by this; these fields only appear on
        // a successful 2xx body. No referenceId here -- that has nothing to reference
        // until true async processing (requirement 7b) exists.
        @JsonView(ResponseViews.Minimal.class) String status,
        @JsonView(ResponseViews.Minimal.class) String code,
        @JsonView(ResponseViews.Minimal.class) String message,
        @JsonView(ResponseViews.Minimal.class) String correlationId
) {
}
