package com.hsm.client.deidjob;

import com.hsm.client.deid.DeidException;
import com.hsm.client.deidjob.source.CsvRecordSource;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CsvRecordSourceTest {

    private static CsvRecordSource csv(String text, boolean skipBadRows) {
        return new CsvRecordSource(new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8,
                ',', '"', skipBadRows);
    }

    @Test
    void readsHeaderAndRecordsAsStrings() {
        try (CsvRecordSource s = csv("member_id,amount,status\nM1,25.00,ACTIVE\nM2,,REDEEMED\n", false)) {
            assertEquals(List.of("member_id", "amount", "status"), s.columns());
            List<Map<String, String>> page = s.nextPage(10);
            assertEquals(2, page.size());
            assertEquals(Map.of("member_id", "M1", "amount", "25.00", "status", "ACTIVE"), page.get(0));
            assertEquals("", page.get(1).get("amount"), "an empty field is empty, not null");
            assertTrue(s.nextPage(10).isEmpty());
        }
    }

    @Test
    void quotedFieldsWithDelimitersQuotesAndLineBreaks() {
        String text = "name,note\r\n\"Doe, Jane\",\"said \"\"hi\"\"\nthen left\"\r\nRaj,plain\r\n";
        try (CsvRecordSource s = csv(text, false)) {
            List<Map<String, String>> page = s.nextPage(10);
            assertEquals("Doe, Jane", page.get(0).get("name"));
            assertEquals("said \"hi\"\nthen left", page.get(0).get("note"));
            assertEquals("plain", page.get(1).get("note"));
        }
    }

    @Test
    void aByteOrderMarkIsSkippedAndTheLastLineNeedsNoBreak() {
        try (CsvRecordSource s = csv("﻿id,zip\n1,60614", false)) {
            assertEquals(List.of("id", "zip"), s.columns());
            assertEquals("60614", s.nextPage(10).get(0).get("zip"));
        }
    }

    @Test
    void pagesAreReturnedInOrder() {
        StringBuilder text = new StringBuilder("id\n");
        for (int i = 1; i <= 25; i++) {
            text.append(i).append('\n');
        }
        try (CsvRecordSource s = csv(text.toString(), false)) {
            assertEquals("1", s.nextPage(10).get(0).get("id"));
            assertEquals("11", s.nextPage(10).get(0).get("id"));
            assertEquals(5, s.nextPage(10).size());
            assertTrue(s.nextPage(10).isEmpty());
        }
    }

    @Test
    void aWrongFieldCountFailsNamingTheRowNotTheValues() {
        try (CsvRecordSource s = csv("id,email\n1,a@x.com\n2,jane.doe@mail.com,extra\n", false)) {
            DeidException e = assertThrows(DeidException.class, () -> s.nextPage(10));
            assertTrue(e.getMessage().contains("CSV row 2"), e.getMessage());
            assertFalse(e.getMessage().contains("jane.doe"));
        }
    }

    @Test
    void skipBadRowsCountsThem() {
        try (CsvRecordSource s = csv("id,email\n1,a@x.com\n2\n3,c@x.com\n", true)) {
            assertEquals(2, s.nextPage(10).size());
            assertEquals(1, s.skipped());
        }
    }

    @Test
    void headerProblemsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> csv("", false));
        assertThrows(IllegalArgumentException.class, () -> csv("id,id\n1,2\n", false));
        assertThrows(IllegalArgumentException.class, () -> csv("id,,x\n", false));
    }

    @Test
    void anUnclosedQuoteFails() {
        try (CsvRecordSource s = csv("id,note\n1,\"open\n", false)) {
            assertThrows(DeidException.class, () -> s.nextPage(10));
        }
    }

    @Test
    void otherDelimiters() {
        try (CsvRecordSource s = new CsvRecordSource(new ByteArrayInputStream("a|b\n1|x,y\n".getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8, '|', '"', false)) {
            assertEquals("x,y", s.nextPage(1).get(0).get("b"));
        }
    }
}
