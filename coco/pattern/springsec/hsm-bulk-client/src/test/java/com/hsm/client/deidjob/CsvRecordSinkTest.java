package com.hsm.client.deidjob;

import com.hsm.client.deidjob.sink.CsvRecordSink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CsvRecordSinkTest {

    @TempDir
    Path dir;

    private String write(CsvRecordSink sink, List<String> header, List<List<String>> rows) throws Exception {
        sink.writeHeader(header);
        sink.writeRows(rows);
        sink.commit();
        return Files.readString(sink.target(), StandardCharsets.UTF_8);
    }

    @Test
    void quotesOnlyWhatNeedsQuoting() throws Exception {
        CsvRecordSink sink = new CsvRecordSink(dir.resolve("out.csv"), ',', false, true);
        String text = write(sink, List.of("a", "b", "c"), List.of(
                List.of("plain", "with,comma", "with \"quote\""),
                Arrays.asList(null, "", "two\nlines")));
        assertEquals("a,b,c\nplain,\"with,comma\",\"with \"\"quote\"\"\"\n,,\"two\nlines\"\n", text);
    }

    @Test
    void formulaGuardPrefixesAQuote() throws Exception {
        CsvRecordSink sink = new CsvRecordSink(dir.resolve("out.csv"), ',', false, true);
        String text = write(sink, List.of("v"), List.of(List.of("=SUM(A1)"), List.of("+1"), List.of("-5.00"),
                List.of("@cmd"), List.of("\tx"), List.of("ok")));
        assertEquals("v\n'=SUM(A1)\n'+1\n'-5.00\n'@cmd\n'\tx\nok\n", text);
        assertEquals(5, sink.guarded());
    }

    @Test
    void formulaGuardOffAndNotOnTheHeader() throws Exception {
        CsvRecordSink sink = new CsvRecordSink(dir.resolve("out.csv"), ',', false, false);
        assertEquals("-h\n-5.00\n", write(sink, List.of("-h"), List.of(List.of("-5.00"))));
    }

    @Test
    void crlfAndAnotherDelimiter() throws Exception {
        CsvRecordSink sink = new CsvRecordSink(dir.resolve("out.csv"), '|', true, true);
        assertEquals("a|b\r\nx,y|\"p|q\"\r\n", write(sink, List.of("a", "b"), List.of(List.of("x,y", "p|q"))));
    }

    @Test
    void writesAPartFileAndRenamesOnCommit() throws Exception {
        Path target = dir.resolve("vendor/out.csv");
        CsvRecordSink sink = new CsvRecordSink(target, ',', false, true);
        sink.writeHeader(List.of("a"));
        assertTrue(Files.exists(dir.resolve("vendor/out.csv.part")));
        assertFalse(Files.exists(target), "no .csv until complete");
        sink.commit();
        assertTrue(Files.exists(target));
        assertFalse(Files.exists(dir.resolve("vendor/out.csv.part")));

        String expected = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(target)));
        assertEquals(expected, sink.sha256());
        assertEquals(0, sink.rows());
    }

    @Test
    void abortLeavesNothing() {
        Path target = dir.resolve("out.csv");
        try (CsvRecordSink sink = new CsvRecordSink(target, ',', false, true)) {
            sink.writeHeader(List.of("a"));
            sink.writeRows(List.of(List.of("x")));
        }
        assertFalse(Files.exists(target));
        assertFalse(Files.exists(dir.resolve("out.csv.part")));
    }

    @Test
    void anExistingFileIsNotOverwritten() throws Exception {
        Files.writeString(dir.resolve("out.csv"), "earlier run");
        assertThrows(IllegalStateException.class, () -> new CsvRecordSink(dir.resolve("out.csv"), ',', false, true));
        assertEquals("earlier run", Files.readString(dir.resolve("out.csv")));
    }

    @Test
    void utf8WithoutBom() throws Exception {
        CsvRecordSink sink = new CsvRecordSink(dir.resolve("out.csv"), ',', false, true);
        write(sink, List.of("name"), List.of(List.of("Zoë")));
        byte[] bytes = Files.readAllBytes(dir.resolve("out.csv"));
        assertEquals('n', bytes[0]);
        assertTrue(new String(bytes, StandardCharsets.UTF_8).contains("Zoë"));
    }
}
