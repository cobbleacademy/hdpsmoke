package com.hsm.client.deid.transform;

import com.hsm.client.deid.ColumnTransform;
import com.hsm.client.deid.FieldOptions;
import com.hsm.client.deid.FieldStats;

import java.util.Map;

/**
 * {@code passthrough}: the value is copied unchanged (DEIDENTIFICATION.md section 4.6).
 * The explicit way to let a field through; the dry run lists every passthrough field
 * for the reviewer.
 */
public final class PassthroughTransform implements ColumnTransform {

    public static final PassthroughTransform INSTANCE = new PassthroughTransform();

    private PassthroughTransform() {
    }

    public static void checkOptions(FieldOptions options) {
        options.rejectUnknown("passthrough");
    }

    @Override
    public Method method() {
        return Method.PASSTHROUGH;
    }

    @Override
    public String apply(String value, Map<String, String> record, FieldStats stats) {
        return value;
    }
}
