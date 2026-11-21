package com.hsm.client.deid;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * A field's method options as written in the profile ({@code keep-last: 4},
 * {@code to: year}), read with types. Every option read is remembered, so
 * {@link #rejectUnknown} can refuse a misspelt option instead of silently ignoring it:
 * {@code keep_last: 4} masking the whole card number would be a surprise, but
 * {@code keep-lst: 4} sending it unmasked would be worse.
 *
 * <p>Errors are {@link IllegalArgumentException}s without the field name;
 * {@link TransformRegistry} adds it.
 */
public final class FieldOptions {

    private final Map<String, Object> raw;
    private final Set<String> read = new HashSet<>();

    public FieldOptions(Map<String, Object> raw) {
        this.raw = raw == null ? Map.of() : raw;
    }

    public boolean has(String key) {
        read.add(key);
        return raw.containsKey(key) && raw.get(key) != null;
    }

    public String string(String key, String defaultValue) {
        read.add(key);
        Object v = raw.get(key);
        if (v == null) {
            return defaultValue;
        }
        if (v instanceof Map || v instanceof List) {
            throw new IllegalArgumentException("option '" + key + "' must be a single value");
        }
        return String.valueOf(v);
    }

    public int integer(String key, int defaultValue, int min, int max) {
        read.add(key);
        Object v = raw.get(key);
        if (v == null) {
            return defaultValue;
        }
        int n;
        if (v instanceof Number num && num.doubleValue() == Math.rint(num.doubleValue())) {
            n = num.intValue();
        } else {
            try {
                n = Integer.parseInt(String.valueOf(v).trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("option '" + key + "' must be a whole number");
            }
        }
        if (n < min || n > max) {
            throw new IllegalArgumentException("option '" + key + "' must be between " + min + " and " + max);
        }
        return n;
    }

    public List<String> stringList(String key) {
        read.add(key);
        Object v = raw.get(key);
        if (v == null) {
            return List.of();
        }
        if (!(v instanceof List<?> list)) {
            throw new IllegalArgumentException("option '" + key + "' must be a list, e.g. [\"-\"]");
        }
        List<String> out = new ArrayList<>();
        for (Object o : list) {
            out.add(String.valueOf(o));
        }
        return out;
    }

    /** Throws if the profile set an option no {@code read} call asked for. */
    public void rejectUnknown(String method) {
        Set<String> unknown = new TreeSet<>(raw.keySet());
        unknown.removeAll(read);
        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException("unknown option(s) for " + method + ": " + String.join(", ", unknown));
        }
    }
}
