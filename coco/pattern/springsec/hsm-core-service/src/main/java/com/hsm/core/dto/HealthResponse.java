package com.hsm.core.dto;

/**
 * kekOk / kekExpiresAt / kekDaysLeft cover the latest version of every KEK in use
 * (see KekExpiryMonitor). kekOk is false when any of them is expired, disabled, not
 * yet valid or unreadable -- new encrypts under that KEK fail -- and status is then
 * "degraded". kekExpiresAt / kekDaysLeft describe the one that expires first (null
 * when none has an expiry). KEK names are deliberately left out: this endpoint is public.
 */
public record HealthResponse(String status, boolean vaultReachable, boolean dbReachable,
                             boolean kekOk, String kekExpiresAt, Long kekDaysLeft) {
}
