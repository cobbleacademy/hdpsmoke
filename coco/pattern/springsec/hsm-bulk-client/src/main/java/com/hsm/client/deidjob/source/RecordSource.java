package com.hsm.client.deidjob.source;

import java.util.List;
import java.util.Map;

/**
 * Where the DEID job reads records from (DEIDENTIFICATION.md section 9). A record is a
 * map of source column to the value as a string; a missing or SQL NULL value is null.
 *
 * <p>Implementations never log values.
 */
public interface RecordSource extends AutoCloseable {

    /**
     * Every column the source has, known before the first record: the CSV header or
     * the table's metadata. The validator checks the profile against it, and the
     * manifest lists the ones the profile doesn't (dropped by default).
     */
    List<String> columns();

    /**
     * The next records, in source order.
     *
     * @param max at most this many
     * @return an empty list when there are no more
     */
    List<Map<String, String>> nextPage(int max);

    /** Records skipped as bad (CSV {@code on-bad-row: skip}); 0 for sources that never skip. */
    default long skipped() {
        return 0;
    }

    @Override
    void close();
}
