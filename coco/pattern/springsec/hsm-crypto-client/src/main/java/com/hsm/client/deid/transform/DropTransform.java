package com.hsm.client.deid.transform;

import com.hsm.client.deid.ColumnTransform;
import com.hsm.client.deid.FieldOptions;
import com.hsm.client.deid.FieldStats;

import java.util.Map;

/**
 * {@code drop}: the field is left out of the CSV (DEIDENTIFICATION.md section 4.1).
 * {@link com.hsm.client.deid.DeidPipeline} never writes a column for it, so
 * {@link #apply} is never called in a run; it returns null to be safe.
 */
public final class DropTransform implements ColumnTransform {

    public static final DropTransform INSTANCE = new DropTransform();

    private DropTransform() {
    }

    public static void checkOptions(FieldOptions options) {
        options.rejectUnknown("drop");
    }

    @Override
    public Method method() {
        return Method.DROP;
    }

    @Override
    public String apply(String value, Map<String, String> record, FieldStats stats) {
        return null;
    }
}
