package com.hsm.core.dto;

import com.fasterxml.jackson.annotation.JsonView;
import com.hsm.core.web.ResponseViews;

import java.util.List;

// See BatchEncryptResponse's comment re: why items needs an explicit @JsonView.
public record BatchDecryptResponse(
        @JsonView(ResponseViews.Minimal.class) List<BatchDecryptResultItem> items
) {
}
