package com.hsm.core.dto;

public record HealthResponse(String status, boolean vaultReachable, boolean dbReachable) {
}
