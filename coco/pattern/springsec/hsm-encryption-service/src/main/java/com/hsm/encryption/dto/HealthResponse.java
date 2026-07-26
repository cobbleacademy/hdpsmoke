package com.hsm.encryption.dto;

public record HealthResponse(String status, boolean vaultReachable, boolean dbReachable) {
}
