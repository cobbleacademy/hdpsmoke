package com.hsm.client.deid.transform;

import com.hsm.client.deid.DeidException;

import java.util.Locale;

/**
 * How a value is normalized before {@code tokenize} or a surrogate lookup, so that
 * {@code "M1234567 "} and {@code "M1234567"} are the same member. Option
 * {@code normalize}; {@code trim} by default.
 */
public enum Normalize {
    /** Exactly as read. */
    NONE,
    /** Leading and trailing whitespace removed. */
    TRIM,
    /** Trimmed, then lower-cased (locale-independent): for emails and codes. */
    LOWER,
    /** Only the digits 0-9 kept: for card numbers and phone numbers with separators. */
    DIGITS;

    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static Normalize fromLabel(String label) {
        for (Normalize n : values()) {
            if (n.label().equals(label)) {
                return n;
            }
        }
        throw new IllegalArgumentException("option 'normalize' must be one of none, trim, lower, digits");
    }

    /** Never returns null for a non-null value; fails when nothing is left to identify. */
    public String apply(String value) {
        String out = switch (this) {
            case NONE -> value;
            case TRIM -> value.strip();
            case LOWER -> value.strip().toLowerCase(Locale.ROOT);
            case DIGITS -> value.replaceAll("[^0-9]", "");
        };
        if (out.isEmpty()) {
            throw DeidException.badValue("nothing left after normalize: " + label());
        }
        return out;
    }
}
