package com.hsm.client.deidjob;

import com.hsm.client.deid.DeidException;
import com.hsm.client.deid.DeidValidationException;
import com.hsm.client.deidjob.source.JsonRecordSource;
import com.hsm.client.deidjob.source.JsonRecordSource.Layout;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonRecordSourceTest {

    private static JsonRecordSource source(String text, Layout layout, boolean skip, List<String> paths) {
        return new JsonRecordSource(new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8,
                layout, skip, paths, paths);
    }

    private static List<Map<String, String>> all(JsonRecordSource s, int pageSize) {
        List<Map<String, String>> out = new ArrayList<>();
        List<Map<String, String>> page;
        while (!(page = s.nextPage(pageSize)).isEmpty()) {
            assertTrue(page.size() <= pageSize);
            out.addAll(page);
        }
        return out;
    }

    private static Map<String, String> map(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void jsonLinesWithNestedPathsAndValueTypes() {
        String text = """
                {"id":1,"name":{"first":"Jane"},"amount":25.00,"active":true,"big":12345678901234567890,"zip":"60614"}

                {"id":2,"name":null,"amount":-5,"active":false,"zip":null}
                {"id":3}
                """;
        try (JsonRecordSource s = source(text, Layout.LINES, false, List.of("id", "name.first", "amount", "active", "big", "zip"))) {
            List<Map<String, String>> r = all(s, 2);
            assertEquals(3, r.size());
            assertEquals(map("id", "1", "name.first", "Jane", "amount", "25.00", "active", "true",
                    "big", "12345678901234567890", "zip", "60614"), r.get(0));
            assertEquals(map("id", "2", "name.first", null, "amount", "-5", "active", "false", "big", null, "zip", null), r.get(1));
            assertNull(r.get(2).get("name.first"));
            assertEquals(List.of(), s.pathsNotSeen());
        }
    }

    @Test
    void anArrayFileGivesTheSameRecords() {
        String lines = "{\"a\":1,\"b\":{\"c\":\"x\"}}\n{\"a\":2,\"b\":{\"c\":\"y\"}}\n";
        String array = "﻿[\n  {\"a\":1,\"b\":{\"c\":\"x\"}},\n  {\"a\":2,\"b\":{\"c\":\"y\"}}\n]\n";
        List<String> paths = List.of("a", "b.c");
        try (JsonRecordSource l = source(lines, Layout.LINES, false, paths);
             JsonRecordSource a = source(array, Layout.ARRAY, false, paths)) {
            assertEquals(all(l, 10), all(a, 1));
        }
        try (JsonRecordSource empty = source("[]", Layout.ARRAY, false, paths)) {
            assertEquals(List.of(), all(empty, 10));
        }
    }

    @Test
    void anArrayIsExpandedOneRecordPerElementAndAnEmptyOneKeepsTheParent() {
        String text = """
                {"m":"A","cards":[{"n":"1","amt":5},{"n":"2","amt":6},{"n":"3"}]}
                {"m":"B","cards":[]}
                {"m":"C"}
                {"m":"D","cards":[null]}
                """;
        try (JsonRecordSource s = source(text, Layout.LINES, false, List.of("m", "cards[].n", "cards[].amt"))) {
            List<Map<String, String>> r = all(s, 2);
            assertEquals(List.of(
                    map("m", "A", "cards[].n", "1", "cards[].amt", "5"),
                    map("m", "A", "cards[].n", "2", "cards[].amt", "6"),
                    map("m", "A", "cards[].n", "3", "cards[].amt", null),
                    map("m", "B", "cards[].n", null, "cards[].amt", null),
                    map("m", "C", "cards[].n", null, "cards[].amt", null),
                    map("m", "D", "cards[].n", null, "cards[].amt", null)), r);
        }
    }

    @Test
    void anArrayOfPlainValues() {
        try (JsonRecordSource s = source("{\"id\":1,\"tags\":[\"x\",\"y\"]}\n", Layout.LINES, false, List.of("id", "tags[]"))) {
            assertEquals(List.of(map("id", "1", "tags[]", "x"), map("id", "1", "tags[]", "y")), all(s, 10));
        }
    }

    @Test
    void pathProblemsAreValidationProblems() {
        DeidValidationException e = assertThrows(DeidValidationException.class,
                () -> source("", Layout.LINES, false, List.of("a..b", "x[].y", "z[].w", "p[].q[].r")));
        assertEquals(4, e.problems().size(), e.problems().toString());
        assertTrue(e.problems().get(0).contains("'a..b'"));
        assertTrue(e.problems().stream().anyMatch(p -> p.contains("only one array")));
        assertTrue(e.problems().stream().anyMatch(p -> p.contains("one array, not 2")));
    }

    @Test
    void badRecordsFailNamingTheRecordAndLineButNoValue() {
        String text = "{\"id\":1}\n{\"id\":\"SECRET-1\",bad}\n";
        try (JsonRecordSource s = source(text, Layout.LINES, false, List.of("id"))) {
            DeidException e = assertThrows(DeidException.class, () -> all(s, 10));
            assertTrue(e.getMessage().contains("JSON record 2 (line 2) isn't valid JSON"), e.getMessage());
            assertFalse(e.getMessage().contains("SECRET"), e.getMessage());
            assertFalse(e.getMessage().contains("bad}"), e.getMessage());
        }
        for (String rec : List.of("[1,2]", "\"SECRET-2\"", "{\"id\":{\"x\":1}}", "{\"id\":1,\"id\":2}")) {
            try (JsonRecordSource s = source(rec + "\n", Layout.LINES, false, List.of("id"))) {
                DeidException e = assertThrows(DeidException.class, () -> all(s, 10), rec);
                assertTrue(e.getMessage().startsWith("JSON record 1 (line 1)"), e.getMessage());
                assertFalse(e.getMessage().contains("SECRET"), e.getMessage());
            }
        }
    }

    @Test
    void badRecordsCanBeSkippedAndCounted() {
        String text = "{\"id\":1}\nnot json SECRET\n[1]\n{\"id\":{\"x\":1}}\n{\"id\":5}\n";
        try (JsonRecordSource s = source(text, Layout.LINES, true, List.of("id"))) {
            assertEquals(List.of(map("id", "1"), map("id", "5")), all(s, 10));
            assertEquals(3, s.skipped());
        }
    }

    @Test
    void invalidJsonInAnArrayFileAlwaysFails() {
        try (JsonRecordSource s = source("[{\"id\":1},{\"id\":SECRET}]", Layout.ARRAY, true, List.of("id"))) {
            DeidException e = assertThrows(DeidException.class, () -> all(s, 10));
            assertTrue(e.getMessage().contains("always fails"), e.getMessage());
            assertFalse(e.getMessage().contains("SECRET"), e.getMessage());
        }
        try (JsonRecordSource s = source("[{\"id\":1},5,{\"id\":2}]", Layout.ARRAY, true, List.of("id"))) {
            assertEquals(List.of(map("id", "1"), map("id", "2")), all(s, 10));
            assertEquals(1, s.skipped(), "a non-object element can be skipped");
        }
        assertThrows(DeidException.class, () -> source("{\"id\":1}", Layout.ARRAY, false, List.of("id")));
        try (JsonRecordSource s = source("[{\"id\":1}] x", Layout.ARRAY, false, List.of("id"))) {
            assertThrows(DeidException.class, () -> all(s, 10));
        }
    }

    @Test
    void columnsGrowWithEverythingSeenAndPathsNotSeenAreReported() {
        String text = """
                {"id":1,"name":{"first":"J","last":"D"},"cards":[{"n":"1","pin":"9"}]}
                {"id":2,"extra":null,"tags":["x"]}
                """;
        try (JsonRecordSource s = source(text, Layout.LINES, false, List.of("id", "cards[].n", "nmae.first"))) {
            assertEquals(List.of("id", "cards[].n", "nmae.first"), s.columns(), "before reading: the profile's paths");
            all(s, 10);
            assertEquals(List.of("id", "cards[].n", "nmae.first", "name.first", "name.last", "cards[].pin", "extra", "tags[]"),
                    s.columns());
            assertEquals(List.of("nmae.first"), s.pathsNotSeen());
        }
    }
}
