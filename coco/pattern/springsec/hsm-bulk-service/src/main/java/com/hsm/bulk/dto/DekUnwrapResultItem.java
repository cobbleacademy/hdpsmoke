package com.hsm.bulk.dto;

import java.util.UUID;

public record DekUnwrapResultItem(
        String key,
        String status,
        UUID edekId,
        String wrappedDekB64,
        String detail
) {
    public static DekUnwrapResultItem success(String key, UUID edekId, String wrappedDekB64) {
        return new DekUnwrapResultItem(key, "success", edekId, wrappedDekB64, null);
    }

    public static DekUnwrapResultItem error(String key, String detail) {
        return new DekUnwrapResultItem(key, "error", null, null, detail);
    }
}
