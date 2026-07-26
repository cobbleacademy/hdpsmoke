package com.hsm.core.dto;

import jakarta.validation.constraints.NotBlank;

public record AppStatusRequest(@NotBlank String appId, boolean active) {
}
