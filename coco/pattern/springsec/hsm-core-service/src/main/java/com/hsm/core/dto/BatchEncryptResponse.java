package com.hsm.core.dto;

import com.fasterxml.jackson.annotation.JsonView;
import com.hsm.core.web.ResponseViews;

import java.util.List;

// @JsonView(Minimal.class) on items: this type is inside ResponseDetailBodyAdvice's
// filtered set (so its embedded EncryptResponse items respect X-Response-Detail), which
// means a view is always active here too -- Jackson 3's MapperFeature.DEFAULT_VIEW_INCLUSION
// defaults to false, so an unannotated field would otherwise vanish along with it.
public record BatchEncryptResponse(
        @JsonView(ResponseViews.Minimal.class) List<BatchEncryptResultItem> items
) {
}
