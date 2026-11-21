package com.hsm.client.deid;

import com.hsm.client.deid.transform.GeneralizeTransform;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static com.hsm.client.deid.DeidTestSupport.CLOCK_2026;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeneralizeTransformTest {

    private static GeneralizeTransform generalize(Map<String, Object> options, Clock clock) {
        return GeneralizeTransform.create(new FieldOptions(options), clock, TransformContext.defaultRestrictedZip3());
    }

    private static String run(Map<String, Object> options, String value) {
        return generalize(options, CLOCK_2026).apply(value, Map.of(), new FieldStats());
    }

    @Test
    void yearFromDatesAndDateTimes() {
        Map<String, Object> o = Map.of("to", "year");
        assertEquals("2026", run(o, "2026-10-14"));
        assertEquals("2026", run(o, "2026-10-14T09:15:00"));
        assertEquals("2026", run(o, "2026-10-14T09:15:00+05:30"));
        assertEquals("2025", run(o, "2025-12-31T23:59:59Z"));
        assertEquals("1958", run(Map.of("to", "year", "input-format", "MM/dd/yyyy"), "03/12/1958"));
    }

    @Test
    void birthYearCapsAgesOver89() {
        Map<String, Object> o = Map.of("to", "birth-year");
        // Run in 2026: cut-off 1936.
        assertEquals("1958", run(o, "1958-03-12"));
        assertEquals("1937", run(o, "1937-01-01"));
        assertEquals("<=1936", run(o, "1936-12-31"), "years only: may still be 89, capped anyway");
        assertEquals("<=1936", run(o, "1933-07-30"));
        assertEquals("<=1936", run(o, "1901-01-01"));
    }

    @Test
    void birthYearCutoffMovesAtTheYearBoundary() {
        Map<String, Object> o = Map.of("to", "birth-year");
        Clock lastDay2026 = Clock.fixed(Instant.parse("2026-12-31T23:59:59Z"), ZoneOffset.UTC);
        Clock firstDay2027 = Clock.fixed(Instant.parse("2027-01-01T00:00:00Z"), ZoneOffset.UTC);
        assertEquals("1937", generalize(o, lastDay2026).apply("1937-06-01", Map.of(), new FieldStats()));
        assertEquals("<=1937", generalize(o, firstDay2027).apply("1937-06-01", Map.of(), new FieldStats()));
    }

    @Test
    void birthYearOptionsAndCount() {
        FieldStats stats = new FieldStats();
        GeneralizeTransform t = generalize(Map.of("to", "birth-year", "over-89-as", "90+"), CLOCK_2026);
        assertEquals("90+", t.apply("1933-07-30", Map.of(), stats));
        assertEquals("", generalize(Map.of("to", "birth-year", "over-89-as", ""), CLOCK_2026)
                .apply("1933-07-30", Map.of(), stats));
        assertEquals(2, stats.counter("capped"));
        assertEquals(1936, t.describe().get("cutoff_year"));
    }

    @Test
    void birthYearAsOfAnotherFieldOfTheRecord() {
        GeneralizeTransform t = generalize(Map.of("to", "birth-year", "as-of", "issued"), CLOCK_2026);
        // Age at issue in 2020: cut-off 1930.
        assertEquals("1933", t.apply("1933-07-30", Map.of("issued", "2020-05-01"), new FieldStats()));
        assertEquals("<=1930", t.apply("1929-01-01", Map.of("issued", "2020-05-01"), new FieldStats()));
        // No as-of value: falls back to the run date.
        assertEquals("<=1936", t.apply("1933-07-30", Map.of(), new FieldStats()));
        DeidException e = assertThrows(DeidException.class,
                () -> t.apply("1933-07-30", Map.of("issued", "soon"), new FieldStats()));
        assertTrue(e.getMessage().contains("as-of"));
    }

    @Test
    void zip3WithRestrictedAreasAsZeros() {
        FieldStats stats = new FieldStats();
        GeneralizeTransform t = generalize(Map.of("to", "zip3"), CLOCK_2026);
        assertEquals("606", t.apply("60614", Map.of(), stats));
        assertEquals("941", t.apply("94107-1234", Map.of(), stats));
        assertEquals("941", t.apply("941071234", Map.of(), stats));
        assertEquals("000", t.apply("03601", Map.of(), stats));
        assertEquals("000", t.apply("89301", Map.of(), stats));
        assertEquals(2, stats.counter("restricted"));
        assertThrows(DeidException.class, () -> t.apply("6061", Map.of(), stats));
        assertThrows(DeidException.class, () -> t.apply("ABCDE", Map.of(), stats));
    }

    @Test
    void agesCapAt90() {
        assertEquals("89", run(Map.of("to", "age-90"), "89"));
        assertEquals("90+", run(Map.of("to", "age-90"), "90"));
        assertEquals("90+", run(Map.of("to", "age-90"), "92"));
        assertEquals("40-49", run(Map.of("to", "age-band"), "43"));
        assertEquals("85-89", run(Map.of("to", "age-band", "width", 5), "85"));
        assertEquals("84-89", run(Map.of("to", "age-band", "width", 7), "88"), "band clipped below 90");
        assertEquals("90+", run(Map.of("to", "age-band"), "95"));
        assertThrows(DeidException.class, () -> run(Map.of("to", "age-90"), "-1"));
        assertThrows(DeidException.class, () -> run(Map.of("to", "age-90"), "forty"));
    }

    @Test
    void unparseableValuesFailWithoutTheValue() {
        DeidException e = assertThrows(DeidException.class, () -> run(Map.of("to", "year"), "12th March 1958"));
        assertFalse(e.getMessage().contains("1958"), e.getMessage());
    }

    @Test
    void nullAndEmptyPassThrough() {
        assertNull(run(Map.of("to", "year"), null));
        assertEquals("", run(Map.of("to", "zip3"), ""));
    }

    @Test
    void badOptionsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> run(Map.of(), "x"));
        assertThrows(IllegalArgumentException.class, () -> run(Map.of("to", "month"), "x"));
        assertThrows(IllegalArgumentException.class, () -> run(Map.of("to", "zip3", "width", 5), "x"));
        assertThrows(IllegalArgumentException.class, () -> run(Map.of("to", "year", "as-of", "issued"), "x"),
                "as-of is only for birth-year");
    }
}
