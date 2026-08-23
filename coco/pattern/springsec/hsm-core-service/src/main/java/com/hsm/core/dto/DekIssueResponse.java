package com.hsm.core.dto;

import java.util.List;

public record DekIssueResponse(
        List<DekIssueResultItem> items
) {
}
