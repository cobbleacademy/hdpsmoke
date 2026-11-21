package com.hsm.client.deid;

/**
 * A de-identification run failed on a value or a key. The message names the field
 * and row number, <b>never the value</b>: it ends up in logs (DEIDENTIFICATION.md
 * principle 5).
 *
 * <p>A transform throws {@link #badValue} without a location; the page loop adds the
 * index within the page, and {@link DeidPipeline} turns that into a field and row.
 */
public class DeidException extends RuntimeException {

    private final String reason;
    private final String field;
    private final long row;
    private final int pageIndex;

    private DeidException(String reason, String field, long row, int pageIndex, Throwable cause) {
        super(message(reason, field, row), cause);
        this.reason = reason;
        this.field = field;
        this.row = row;
        this.pageIndex = pageIndex;
    }

    /** A value can't be transformed. {@code reason} must not contain the value. */
    public static DeidException badValue(String reason) {
        return new DeidException(reason, null, -1, -1, null);
    }

    public static DeidException badValue(String reason, Throwable cause) {
        return new DeidException(reason, null, -1, -1, cause);
    }

    /** The same failure, at position {@code index} of the page being processed. */
    public DeidException atPageIndex(int index) {
        return new DeidException(reason, field, row, index, getCause());
    }

    /** The same failure, located at a field and a 1-based row number. */
    public DeidException at(String field, long row) {
        return new DeidException(reason, field, row, pageIndex, getCause());
    }

    public String reason() {
        return reason;
    }

    public String field() {
        return field;
    }

    /** 1-based row number, or -1 when not yet located. */
    public long row() {
        return row;
    }

    /** Index within the current page, or -1. */
    public int pageIndex() {
        return pageIndex;
    }

    private static String message(String reason, String field, long row) {
        if (field == null) {
            return reason;
        }
        return "field '" + field + "'" + (row > 0 ? ", row " + row : "") + ": " + reason;
    }
}
