package com.hsm.encryption.dto;

public record RotateKekResponse(String newKekVersion, int recordsQueued) {
}
