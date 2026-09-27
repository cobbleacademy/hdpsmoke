package com.hsm.fileservice.web;

import java.util.ArrayList;
import java.util.List;

/**
 * Reads the immediate caller's SPIFFE id from Istio's {@code x-forwarded-client-cert}
 * header (set by the sidecar from the mTLS peer certificate). With the default
 * forward-client-cert behaviour the sidecar replaces any client-supplied value, and
 * the last element is always the peer that opened the connection to this pod.
 *
 * <p>Only meaningful inside the mesh with STRICT mTLS. The Istio AuthorizationPolicy
 * in the chart is the primary control; this is a second, in-process check.
 */
public final class CallerIdentity {

    public static final String XFCC_HEADER = "x-forwarded-client-cert";

    private CallerIdentity() {
    }

    /** SPIFFE URI of the immediate peer, or null if the header is absent or has no URI. */
    public static String immediatePeerSpiffeId(String xfcc) {
        if (xfcc == null || xfcc.isBlank()) {
            return null;
        }
        String last = lastElement(xfcc);
        for (String field : splitOutsideQuotes(last, ';')) {
            int eq = field.indexOf('=');
            if (eq > 0 && field.substring(0, eq).trim().equalsIgnoreCase("URI")) {
                String value = field.substring(eq + 1).trim();
                if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
                    value = value.substring(1, value.length() - 1);
                }
                return value.isEmpty() ? null : value;
            }
        }
        return null;
    }

    private static String lastElement(String xfcc) {
        var elements = splitOutsideQuotes(xfcc, ',');
        return elements.isEmpty() ? xfcc : elements.get(elements.size() - 1);
    }

    private static List<String> splitOutsideQuotes(String s, char sep) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') {
                quoted = !quoted;
            }
            if (c == sep && !quoted) {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        out.add(cur.toString());
        return out;
    }
}
