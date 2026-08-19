package com.hsm.core.web;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Resolves the X-Response-Detail request header controlling how much detail
 * /encrypt and /decrypt responses expose -- "full" selects ResponseViews.Full
 * (adds informational/audit fields), anything else or absent selects
 * ResponseViews.Minimal (just what a real caller needs). See ResponseViews
 * and ResponseDetailBodyAdvice (the actual serializer hook).
 */
final class ResponseDetailResolver {

    static final String HEADER = "X-Response-Detail";
    static final String FULL = "full";

    private ResponseDetailResolver() {
    }

    static Class<?> resolve(HttpServletRequest request) {
        String value = request.getHeader(HEADER);
        return FULL.equalsIgnoreCase(value) ? ResponseViews.Full.class : ResponseViews.Minimal.class;
    }
}
