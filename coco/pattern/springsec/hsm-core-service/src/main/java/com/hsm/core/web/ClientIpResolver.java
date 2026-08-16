package com.hsm.core.web;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Resolves the calling application's IP for the audit trail. Traffic never
 * arrives directly from an end user here -- it's always proxied (load
 * balancer, API gateway, etc.) -- so HttpServletRequest.getRemoteAddr() alone
 * only ever captures the last hop's own address, never anything useful for
 * SIEM correlation. X-Forwarded-For's first entry is the originating caller;
 * every entry after it is an intermediate hop (proxy, gateway) appended as
 * the request passed through, assuming each hop in the chain sets the header
 * rather than overwriting it.
 *
 * <p>Trust note: this only makes sense behind an edge that itself sets (or
 * overwrites) X-Forwarded-For unconditionally -- otherwise a direct caller
 * could supply an arbitrary value here and have it land in the audit log as
 * if it were their real address.
 */
public final class ClientIpResolver {

    private static final String FORWARDED_FOR_HEADER = "X-Forwarded-For";

    private ClientIpResolver() {
    }

    public static String resolve(HttpServletRequest request) {
        String xff = request.getHeader(FORWARDED_FOR_HEADER);
        if (xff != null && !xff.isBlank()) {
            return xff.split(",")[0].trim();
        }
        String remoteAddr = request.getRemoteAddr();
        return remoteAddr != null ? remoteAddr : "";
    }
}
