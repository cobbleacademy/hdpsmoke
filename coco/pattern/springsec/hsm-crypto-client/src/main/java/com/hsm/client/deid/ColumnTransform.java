package com.hsm.client.deid;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * One field's method, configured and ready to run. Built by {@link TransformRegistry}
 * from the profile; run page by page by {@link DeidPipeline}.
 *
 * <p>Null in gives null out, and an empty string stays empty. A value that can't be
 * transformed throws {@link DeidException#badValue} with a reason that never contains
 * the value.
 */
public interface ColumnTransform {

    /** The methods a profile can name (DEIDENTIFICATION.md section 4). */
    enum Method {
        DROP, MASK, GENERALIZE, TOKENIZE, SURROGATE, PASSTHROUGH;

        public String label() {
            return name().toLowerCase(Locale.ROOT);
        }

        /**
         * The id recorded in the manifest, e.g. {@code mask@v1}. The version changes
         * if a method's output for the same input and options ever changes.
         */
        public String id() {
            return label() + "@v1";
        }

        public static Method fromLabel(String label) {
            return Arrays.stream(values())
                    .filter(m -> m.label().equals(label))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("unknown method '" + label
                            + "' (expected drop, mask, generalize, tokenize, surrogate or passthrough)"));
        }
    }

    Method method();

    /**
     * Transforms one value.
     *
     * @param value  the source value, already decrypted for a {@code decrypt: true} field
     * @param record the whole source record as read, for options that look at another
     *               field ({@code as-of}); never written anywhere
     * @param stats  counters for the manifest
     */
    String apply(String value, Map<String, String> record, FieldStats stats);

    /**
     * Transforms a page of values; {@code values.get(i)} came from {@code records.get(i)}.
     * The default calls {@link #apply} for each. A transform that works on the whole
     * page at once (surrogate lookups) overrides it.
     */
    default List<String> applyPage(List<String> values, List<Map<String, String>> records, FieldStats stats) {
        List<String> out = new ArrayList<>(values.size());
        for (int i = 0; i < values.size(); i++) {
            try {
                out.add(apply(values.get(i), records.get(i), stats));
            } catch (DeidException e) {
                throw e.atPageIndex(i);
            }
        }
        return out;
    }

    /** Settings for the manifest, e.g. {@code to=year}. Never values or keys. */
    default Map<String, Object> describe() {
        return Map.of();
    }
}
