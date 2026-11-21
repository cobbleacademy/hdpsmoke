package com.hsm.client.deid;

import com.hsm.client.deid.ColumnTransform.Method;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Runs a profile over records, a page at a time (DEIDENTIFICATION.md section 5):
 * decrypt the {@code decrypt: true} fields, apply each field's method, and return the
 * output rows in the profile's field order. Fields the profile doesn't list, and
 * {@code drop} fields, never reach the output.
 *
 * <p>Source-neutral: a record is a map of source column (or JSON path) to the value as
 * read. Reading sources and writing the CSV are the bulk client's job.
 *
 * <p>Not thread-safe: one pipeline per run, fed pages in order.
 */
public final class DeidPipeline {

    /** One output column. */
    private record Column(DeidProfile.Field field, String output, ColumnTransform transform, FieldStats stats) {
    }

    /**
     * One field's line in the manifest. Counts and settings only: no values, tokens or
     * surrogates.
     */
    public record FieldReport(String source, String output, String phi, String method, Map<String, Object> settings,
                              boolean decrypted, long values, long empty, Map<String, Long> counters) {
    }

    private final DeidProfile profile;
    private final List<Column> columns = new ArrayList<>();
    private final FieldDecryptor decryptor;
    private long rows;

    /**
     * @param decryptor needed only when a field is {@code decrypt: true}; may be null otherwise
     * @throws IllegalStateException when a field needs keys, a surrogate table or a
     *                               decryptor the run doesn't have
     */
    public DeidPipeline(DeidProfile profile, TransformRegistry registry, FieldDecryptor decryptor) {
        this.profile = profile;
        this.decryptor = decryptor;
        for (DeidProfile.Field f : profile.fields()) {
            if (f.method() == Method.DROP) {
                continue;
            }
            if (f.decrypt() && decryptor == null) {
                throw new IllegalStateException("field '" + f.name() + "' is decrypt: true but the run has no decryptor");
            }
            ColumnTransform transform;
            try {
                transform = registry.create(f);
            } catch (IllegalArgumentException | IllegalStateException e) {
                throw new IllegalStateException("field '" + f.name() + "': " + e.getMessage(), e);
            }
            columns.add(new Column(f, profile.outputName(f), transform, new FieldStats()));
        }
        if (columns.isEmpty()) {
            throw new IllegalStateException("the profile exports no fields: every listed field is drop");
        }
    }

    /** Does this profile call hsm-core for keys ({@code tokenize}, {@code surrogate}, {@code decrypt: true})? */
    public static boolean needsHsm(DeidProfile profile) {
        return profile.fields().stream().anyMatch(f -> f.method() != Method.DROP
                && (f.decrypt() || f.method() == Method.TOKENIZE || f.method() == Method.SURROGATE));
    }

    /** Does this profile use the surrogate table? */
    public static boolean needsSurrogateTable(DeidProfile profile) {
        return profile.fields().stream().anyMatch(f -> f.method() == Method.SURROGATE);
    }

    /** The CSV header: output names, in the profile's field order. */
    public List<String> header() {
        return columns.stream().map(Column::output).toList();
    }

    /**
     * Transforms one page.
     *
     * @param records        the page, in source order
     * @param firstRowNumber 1-based number of the page's first record, for error messages
     * @return one output row per record, values in {@link #header()} order
     * @throws DeidException naming the field and row of the first value that fails
     */
    public List<List<String>> process(List<Map<String, String>> records, long firstRowNumber) {
        List<List<String>> columnValues = new ArrayList<>(columns.size());
        for (Column c : columns) {
            String name = c.field().name();
            List<String> values = new ArrayList<>(records.size());
            for (int i = 0; i < records.size(); i++) {
                String v = records.get(i).get(name);
                c.stats().countValue(v);
                if (c.field().decrypt() && v != null && !v.isEmpty()) {
                    try {
                        v = decryptor.decrypt(v);
                    } catch (DeidException e) {
                        throw e.at(name, firstRowNumber + i);
                    }
                    c.stats().increment("decrypted");
                }
                values.add(v);
            }
            List<String> out;
            try {
                out = c.transform().applyPage(values, records, c.stats());
            } catch (DeidException e) {
                long row = e.pageIndex() >= 0 ? firstRowNumber + e.pageIndex() : -1;
                throw e.at(name, row);
            }
            columnValues.add(out);
        }

        List<List<String>> rowsOut = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            List<String> row = new ArrayList<>(columns.size());
            for (List<String> col : columnValues) {
                row.add(col.get(i));
            }
            rowsOut.add(row);
        }
        rows += records.size();
        return rowsOut;
    }

    /**
     * Decrypts the first non-empty value of each {@code decrypt: true} field in a sample,
     * so a missing grant or key fails before anything is written (section 5, step 2).
     * The decrypted values are discarded.
     */
    public void verifyDecrypt(List<Map<String, String>> sample) {
        for (Column c : columns) {
            if (!c.field().decrypt()) {
                continue;
            }
            for (int i = 0; i < sample.size(); i++) {
                String v = sample.get(i).get(c.field().name());
                if (v != null && !v.isEmpty()) {
                    try {
                        decryptor.decrypt(v);
                    } catch (DeidException e) {
                        throw e.at(c.field().name(), i + 1L);
                    }
                    break;
                }
            }
        }
    }

    /** Source columns the profile doesn't list: dropped by default, listed in the manifest for the reviewer. */
    public List<String> droppedByDefault(Collection<String> sourceColumns) {
        Set<String> listed = new TreeSet<>();
        profile.fields().forEach(f -> listed.add(f.name()));
        return sourceColumns.stream().filter(c -> !listed.contains(c)).toList();
    }

    /** Fields copied unchanged, for the dry run's review list. */
    public List<String> passthroughFields() {
        return columns.stream().filter(c -> c.field().method() == Method.PASSTHROUGH).map(c -> c.field().name()).toList();
    }

    /** Records processed so far. */
    public long rows() {
        return rows;
    }

    /** Distinct {@code edek_id}s decrypted so far (none without {@code decrypt: true}). */
    public Set<UUID> decryptedEdekIds() {
        return decryptor == null ? Set.of() : decryptor.edekIdsSeen();
    }

    /** One line per output field, for the manifest. */
    public List<FieldReport> report() {
        List<FieldReport> out = new ArrayList<>();
        for (Column c : columns) {
            DeidProfile.Field f = c.field();
            out.add(new FieldReport(f.name(), c.output(), f.phi() == null ? null : f.phi().label(),
                    f.method().id(), new LinkedHashMap<>(c.transform().describe()), f.decrypt(),
                    c.stats().values(), c.stats().empty(), new LinkedHashMap<>(c.stats().counters())));
        }
        return out;
    }
}
