package com.hsm.core.dto;

import com.fasterxml.jackson.annotation.JsonView;
import com.hsm.core.web.ResponseViews;

import java.util.UUID;

/**
 * Ported from app/models/schemas.py's EncryptResponse. Field visibility is
 * split via {@code @JsonView} (see ResponseViews and EncryptController):
 * minimal (default) is just what a real caller needs -- ciphertext, reused,
 * and the response envelope; full (opt-in via X-Response-Detail: full) adds
 * the informational/audit fields below. The individual binary fields
 * (iv_b64/ciphertext_b64/tag_b64) that used to sit alongside ciphertext for
 * backward compatibility with a pre-token contract are gone entirely, not
 * just hidden -- this service has never had a real external consumer, so
 * there was nothing to stay compatible with.
 *
 * <p>Every field below carries an explicit {@code @JsonView} -- Jackson 3's
 * {@code MapperFeature.DEFAULT_VIEW_INCLUSION} defaults to {@code false}
 * (Jackson 2 defaulted to {@code true}), so an unannotated field is hidden
 * the moment any view at all is active, not shown-by-default the way it used
 * to be. {@code Minimal}-tagged fields still appear under the {@code Full}
 * view too, since {@code Full extends Minimal}.
 */
public record EncryptResponse(
        // Preferred: single opaque token, store and echo back as-is. Always visible --
        // this is the one field every caller actually needs.
        @JsonView(ResponseViews.Minimal.class) String ciphertext, // "v1.<base64url(version|edek_id|iv|tag|ciphertext)>"

        // Informational fields -- useful for logging/audit, not needed for decrypt.
        // Full view only.
        @JsonView(ResponseViews.Full.class) UUID edekId,
        @JsonView(ResponseViews.Full.class) String ownerAppId,
        @JsonView(ResponseViews.Full.class) String algorithm,
        @JsonView(ResponseViews.Full.class) String encoding,
        @JsonView(ResponseViews.Full.class) String kekVersion,

        // true when this call reused an existing named DEK (dekName was set and
        // already had a current row) rather than minting a fresh one -- observability/
        // audit signal, not needed for decrypt. Always false when dekName was unset.
        // Cheap and operationally useful enough to keep in the minimal view.
        @JsonView(ResponseViews.Minimal.class) boolean reused,

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
