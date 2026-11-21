package com.hsm.client.deid;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Counts for one output field, for the manifest and the dry run: how many values,
 * how many null or empty, and method-specific counts such as {@code capped} or
 * {@code new}. Counts only, never values.
 */
public final class FieldStats {

    private long values;
    private long empty;
    private final Map<String, Long> counters = new LinkedHashMap<>();

    void countValue(String value) {
        values++;
        if (value == null || value.isEmpty()) {
            empty++;
        }
    }

    public void increment(String counter) {
        add(counter, 1);
    }

    public void add(String counter, long n) {
        counters.merge(counter, n, Long::sum);
    }

    public long values() {
        return values;
    }

    /** Null or empty source values. */
    public long empty() {
        return empty;
    }

    public long counter(String name) {
        return counters.getOrDefault(name, 0L);
    }

    public Map<String, Long> counters() {
        return Collections.unmodifiableMap(counters);
    }
}
