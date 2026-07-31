package com.hsm.bulk.dto;

import java.util.List;

public record DekIssueResponse(
        List<DekIssueResultItem> items
) {
}
