package com.hsm.client.deidjob.source;

import com.hsm.client.deid.DeidException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A CSV file with a header row (DEIDENTIFICATION.md section 9). RFC 4180: fields may be
 * quoted, a quote inside a quoted field is doubled, and a quoted field may span lines.
 * A UTF-8 byte order mark is skipped. Every value is a string; an empty field is "".
 *
 * <p>A row with the wrong number of fields fails the run, naming the row number only,
 * or is counted and skipped with {@code skipBadRows}. Row numbers count records, not
 * lines, starting at 1 for the first record after the header.
 */
public final class CsvRecordSource implements RecordSource {

    private final Reader reader;
    private final char delimiter;
    private final char quote;
    private final boolean skipBadRows;
    private final List<String> columns;
    private long recordNumber;
    private long skipped;
    private boolean eof;
    private int pushback = -2;

    public CsvRecordSource(InputStream in, Charset charset, char delimiter, char quote, boolean skipBadRows) {
        this.reader = new BufferedReader(new InputStreamReader(in, charset), 64 * 1024);
        this.delimiter = delimiter;
        this.quote = quote;
        this.skipBadRows = skipBadRows;
        if (delimiter == quote || delimiter == '\n' || delimiter == '\r') {
            throw new IllegalArgumentException("the CSV delimiter can't be the quote or a line break");
        }
        skipBom();
        List<String> header = readRow();
        if (header == null || (header.size() == 1 && header.get(0).isBlank())) {
            throw new IllegalArgumentException("the CSV file has no header row (header: true is required)");
        }
        Set<String> seen = new HashSet<>();
        List<String> names = new ArrayList<>();
        for (String h : header) {
            String name = h.strip();
            if (name.isEmpty()) {
                throw new IllegalArgumentException("the CSV header has an empty column name");
            }
            if (!seen.add(name)) {
                throw new IllegalArgumentException("the CSV header names column '" + name + "' more than once");
            }
            names.add(name);
        }
        this.columns = List.copyOf(names);
    }

    @Override
    public List<String> columns() {
        return columns;
    }

    @Override
    public List<Map<String, String>> nextPage(int max) {
        List<Map<String, String>> page = new ArrayList<>(Math.min(max, 4096));
        while (page.size() < max) {
            List<String> row = readRow();
            if (row == null) {
                break;
            }
            if (row.size() == 1 && row.get(0).isEmpty()) {
                continue; // a blank line
            }
            recordNumber++;
            if (row.size() != columns.size()) {
                if (skipBadRows) {
                    skipped++;
                    continue;
                }
                throw DeidException.badValue("CSV row " + recordNumber + " has " + row.size() + " fields, the header has "
                        + columns.size() + " (set on-bad-row: skip to count and skip such rows)");
            }
            Map<String, String> record = new HashMap<>(columns.size() * 2);
            for (int i = 0; i < columns.size(); i++) {
                record.put(columns.get(i), row.get(i));
            }
            page.add(record);
        }
        return page;
    }

    @Override
    public long skipped() {
        return skipped;
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void skipBom() {
        int c = read();
        if (c != '﻿') {
            pushback = c;
        }
    }

    private int read() {
        if (pushback != -2) {
            int c = pushback;
            pushback = -2;
            return c;
        }
        try {
            return reader.read();
        } catch (IOException e) {
            throw new UncheckedIOException("reading the CSV source failed", e);
        }
    }

    /** One row of fields, or null at end of file. A line ending is \n, \r\n or \r. */
    private List<String> readRow() {
        if (eof) {
            return null;
        }
        List<String> fields = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        boolean wasQuoted = false;
        boolean any = false;
        while (true) {
            int c = read();
            if (c == -1) {
                eof = true;
                if (quoted) {
                    throw DeidException.badValue("CSV row " + (recordNumber + 1) + " has an unclosed quote at end of file");
                }
                if (!any && fields.isEmpty()) {
                    return null;
                }
                fields.add(field.toString());
                return fields;
            }
            any = true;
            char ch = (char) c;
            if (quoted) {
                if (ch == quote) {
                    int next = read();
                    if (next == quote) {
                        field.append(quote);
                    } else {
                        quoted = false;
                        pushback = next;
                    }
                } else {
                    field.append(ch);
                }
            } else if (ch == quote && field.isEmpty() && !wasQuoted) {
                quoted = true;
                wasQuoted = true;
            } else if (ch == delimiter) {
                fields.add(field.toString());
                field.setLength(0);
                wasQuoted = false;
            } else if (ch == '\n' || ch == '\r') {
                if (ch == '\r') {
                    int next = read();
                    if (next != '\n') {
                        pushback = next;
                    }
                }
                fields.add(field.toString());
                return fields;
            } else if (wasQuoted) {
                throw DeidException.badValue("CSV row " + (recordNumber + 1) + " has text after a closing quote");
            } else {
                field.append(ch);
            }
        }
    }
}
