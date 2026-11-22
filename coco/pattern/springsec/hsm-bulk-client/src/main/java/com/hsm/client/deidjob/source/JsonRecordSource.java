package com.hsm.client.deidjob.source;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.hsm.client.deid.DeidException;
import com.hsm.client.deid.DeidValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A JSON file (DEIDENTIFICATION.md section 9): JSON Lines ({@code lines}, one object per
 * line) or one top-level array of objects ({@code array}). Read as a stream, one record
 * at a time.
 *
 * <h2>Paths</h2>
 * A profile field names a path (section 6.1): {@code member_id}, {@code customer.zip},
 * {@code orders[].card_number}, or {@code tags[]} for an array of plain values.
 * <ul>
 *   <li>A missing key or a JSON null is a null value.</li>
 *   <li>Strings are read as they are; numbers keep their source text ({@code 25.00} stays
 *       {@code 25.00}); booleans become {@code true} / {@code false}.</li>
 *   <li>A path that reaches an object or array, or passes through a value that isn't an
 *       object, is a bad record.</li>
 *   <li>At most one array may be expanded, so every {@code []} in a profile must be the same
 *       array ({@code orders[]}), once per path. Each element becomes one record that also
 *       carries the parent's other fields. A missing, null or empty array gives one record
 *       with the array fields null, so no parent record disappears from the output.</li>
 * </ul>
 *
 * <h2>Bad records</h2>
 * A record that isn't a JSON object, isn't valid JSON (JSON Lines only), repeats a key or
 * has a bad path fails the run, naming the record number (and line), or is counted and
 * skipped with {@code skipBadRecords}. Invalid JSON inside an {@code array} file always
 * fails: the parser can't find the next record. Messages never quote the source text,
 * so no value reaches a log.
 *
 * <h2>Columns</h2>
 * {@link #columns()} is the profile's paths followed by every value path seen so far, in
 * first-seen order (array elements as {@code orders[].sku}). Read at the end of a run, it
 * is what the manifest lists as dropped by default. Paths the profile names that no record
 * has are in {@link #pathsNotSeen()}.
 */
public final class JsonRecordSource implements RecordSource {

    public enum Layout { LINES, ARRAY }

    private static final Logger log = LoggerFactory.getLogger(JsonRecordSource.class);
    private static final Pattern SEGMENT = Pattern.compile("[^.\\[\\]]+(\\[])?");
    private static final int MAX_SEEN_PATHS = 2000;

    private final ObjectMapper mapper;
    private final Layout layout;
    private final boolean skipBadRecords;
    private final BufferedReader reader;
    private final JsonParser arrayParser;

    /** Paths the records are read for, split into segments. */
    private final List<Path> paths;
    /** The array prefix ({@code orders[]}), or null when no path expands an array. */
    private final List<String> arrayPrefix;

    private final Set<String> seen = new LinkedHashSet<>();
    private final Set<String> notSeen;
    private final List<String> profilePaths;
    private boolean seenCapped;

    private final Deque<Map<String, String>> pending = new ArrayDeque<>();
    private long recordNumber;
    private long lineNumber;
    private long skipped;
    private boolean eof;

    private record Path(String name, List<String> before, List<String> after, boolean expands) {
    }

    /**
     * @param profilePaths the profile's field names, in profile order
     * @param readPaths    the paths each record is read for: the exported fields and the
     *                     fields their options read; a subset of {@code profilePaths}
     * @throws DeidValidationException when a path is malformed or uses more than one array
     */
    public JsonRecordSource(InputStream in, Charset charset, Layout layout, boolean skipBadRecords,
                            List<String> profilePaths, Collection<String> readPaths) {
        this.layout = layout == null ? Layout.LINES : layout;
        this.skipBadRecords = skipBadRecords;
        this.profilePaths = List.copyOf(profilePaths);

        List<String> problems = new ArrayList<>();
        List<String> prefix = null;
        String prefixText = null;
        for (String p : profilePaths) {
            List<String> segments = parse(p, problems);
            if (segments == null) {
                continue;
            }
            int arrays = 0;
            for (int i = 0; i < segments.size(); i++) {
                if (segments.get(i).endsWith("[]")) {
                    arrays++;
                    List<String> thisPrefix = segments.subList(0, i + 1);
                    String text = String.join(".", thisPrefix);
                    if (prefix == null) {
                        prefix = List.copyOf(thisPrefix);
                        prefixText = text;
                    } else if (!prefix.equals(thisPrefix) && arrays == 1) {
                        problems.add("field '" + p + "': only one array can be expanded, and '" + prefixText
                                + "' already is; this path expands '" + text + "'");
                    }
                }
            }
            if (arrays > 1) {
                problems.add("field '" + p + "': a path can expand one array, not " + arrays);
            }
        }
        if (!problems.isEmpty()) {
            throw new DeidValidationException(problems);
        }
        this.arrayPrefix = prefix;

        List<Path> read = new ArrayList<>();
        for (String p : readPaths) {
            List<String> segments = parse(p, new ArrayList<>());
            if (segments == null) {
                continue;
            }
            if (prefix != null && segments.size() >= prefix.size() && segments.subList(0, prefix.size()).equals(prefix)) {
                read.add(new Path(p, strip(prefix), List.copyOf(segments.subList(prefix.size(), segments.size())), true));
            } else {
                read.add(new Path(p, List.copyOf(segments), List.of(), false));
            }
        }
        this.paths = List.copyOf(read);
        this.notSeen = new LinkedHashSet<>(profilePaths);

        JsonFactory factory = JsonFactory.builder()
                .enable(com.fasterxml.jackson.core.StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                .build();
        // Exact decimals: 25.00 stays 25.00. A JSON Lines line must hold one value only;
        // in an array file the next element follows, so that check is off there.
        this.mapper = new ObjectMapper(factory)
                .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true))
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        if (this.layout == Layout.LINES) {
            mapper.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        }
        this.reader = new BufferedReader(new InputStreamReader(in, charset), 64 * 1024);
        skipBom();
        if (this.layout == Layout.ARRAY) {
            try {
                this.arrayParser = factory.createParser(reader);
                arrayParser.setCodec(mapper);
                JsonToken first = arrayParser.nextToken();
                if (first == null) {
                    eof = true;
                } else if (first != JsonToken.START_ARRAY) {
                    throw DeidException.badValue("the JSON file doesn't start with '[' (layout: array); "
                            + "for one object per line use layout: lines");
                }
            } catch (JsonProcessingException e) {
                throw DeidException.badValue("the JSON file isn't valid JSON at " + where(e.getLocation()));
            } catch (IOException e) {
                throw new UncheckedIOException("reading the JSON source failed", e);
            }
        } else {
            this.arrayParser = null;
        }
    }

    /** The segments of a path, or null (with a problem added) when it is malformed. */
    private static List<String> parse(String path, List<String> problems) {
        if (path == null || path.isEmpty() || path.startsWith(".") || path.endsWith(".") || path.contains("..")) {
            problems.add("field '" + path + "': not a valid JSON path (write a.b or a[].b)");
            return null;
        }
        List<String> segments = List.of(path.split("\\.", -1));
        for (String s : segments) {
            if (!SEGMENT.matcher(s).matches()) {
                problems.add("field '" + path + "': not a valid JSON path at '" + s + "' (write a.b or a[].b)");
                return null;
            }
        }
        return segments;
    }

    /** The prefix segments without the trailing [] on the last one. */
    private static List<String> strip(List<String> prefix) {
        List<String> out = new ArrayList<>(prefix);
        String last = out.remove(out.size() - 1);
        out.add(last.substring(0, last.length() - 2));
        return List.copyOf(out);
    }

    @Override
    public List<String> columns() {
        Set<String> all = new LinkedHashSet<>(profilePaths);
        all.addAll(seen);
        return List.copyOf(all);
    }

    /** Profile paths that no record read so far has (not even as null). */
    public List<String> pathsNotSeen() {
        return List.copyOf(notSeen);
    }

    @Override
    public List<Map<String, String>> nextPage(int max) {
        List<Map<String, String>> page = new ArrayList<>(Math.min(max, 4096));
        while (page.size() < max) {
            if (pending.isEmpty() && !readRecord()) {
                break;
            }
            while (!pending.isEmpty() && page.size() < max) {
                page.add(pending.poll());
            }
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
            if (arrayParser != null) {
                arrayParser.close();
            }
            reader.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // ---- reading ----

    /** Reads the next good record into {@link #pending}; false at end of file. */
    private boolean readRecord() {
        while (!eof) {
            JsonNode node = layout == Layout.LINES ? nextLine() : nextElement();
            if (node == null) {
                continue; // a skipped bad record, or a blank line
            }
            try {
                expand(node);
                return true;
            } catch (DeidException e) {
                bad(e.reason());
            }
        }
        return false;
    }

    private JsonNode nextLine() {
        String line;
        try {
            line = reader.readLine();
        } catch (IOException e) {
            throw new UncheckedIOException("reading the JSON source failed", e);
        }
        if (line == null) {
            eof = true;
            return null;
        }
        lineNumber++;
        if (line.isBlank()) {
            return null;
        }
        recordNumber++;
        JsonNode node;
        try {
            node = mapper.readTree(line);
        } catch (JsonProcessingException e) {
            bad(invalid(e));
            return null;
        }
        if (!node.isObject()) {
            bad("is a JSON " + kind(node) + ", not an object");
            return null;
        }
        return node;
    }

    private JsonNode nextElement() {
        try {
            JsonToken t = arrayParser.nextToken();
            if (t == null) {
                throw DeidException.badValue("the JSON array isn't closed at end of file");
            }
            if (t == JsonToken.END_ARRAY) {
                eof = true;
                if (arrayParser.nextToken() != null) {
                    throw DeidException.badValue("the JSON file has content after the closing ']' at "
                            + where(arrayParser.currentLocation()));
                }
                return null;
            }
            recordNumber++;
            if (t != JsonToken.START_OBJECT) {
                arrayParser.skipChildren();
                bad("is a JSON " + kind(t) + ", not an object");
                return null;
            }
            return arrayParser.readValueAsTree();
        } catch (JsonProcessingException e) {
            String reason = isDuplicate(e) ? "repeats a key" : "isn't valid JSON";
            throw DeidException.badValue("JSON record " + (recordNumber == 0 ? 1 : recordNumber) + " " + reason + " at "
                    + where(e.getLocation()) + " (in layout: array the rest of the file can't be read, so this always fails)");
        } catch (IOException e) {
            throw new UncheckedIOException("reading the JSON source failed", e);
        }
    }

    /** Fails, or counts and skips, the current record. */
    private void bad(String reason) {
        String at = "JSON record " + recordNumber + (layout == Layout.LINES ? " (line " + lineNumber + ")" : "");
        if (skipBadRecords) {
            skipped++;
            return;
        }
        throw DeidException.badValue(at + " " + reason + " (set on-bad-row: skip to count and skip such records)");
    }

    /** One record per array element (or one record when nothing expands). */
    private void expand(JsonNode node) {
        record(node, "");
        Map<String, String> base = new HashMap<>(paths.size() * 2);
        for (Path p : paths) {
            if (!p.expands()) {
                base.put(p.name(), value(node, p.before(), p.name()));
            }
        }
        if (arrayPrefix == null) {
            pending.add(base);
            return;
        }
        JsonNode array = descend(node, strip(arrayPrefix), String.join(".", arrayPrefix));
        if (array != null && !array.isArray()) {
            throw DeidException.badValue("'" + String.join(".", arrayPrefix) + "' is a JSON " + kind(array) + ", not an array");
        }
        if (array == null || array.isEmpty()) {
            Map<String, String> r = new HashMap<>(base);
            paths.stream().filter(Path::expands).forEach(p -> r.put(p.name(), null));
            pending.add(r);
            return;
        }
        List<Map<String, String>> out = new ArrayList<>(array.size());
        for (JsonNode element : array) {
            if (!element.isObject() && !element.isNull() && paths.stream().anyMatch(p -> p.expands() && !p.after().isEmpty())) {
                throw DeidException.badValue("an element of '" + String.join(".", arrayPrefix) + "' is a JSON "
                        + kind(element) + ", not an object");
            }
            Map<String, String> r = new HashMap<>(base);
            for (Path p : paths) {
                if (p.expands()) {
                    if (p.after().isEmpty()) {
                        r.put(p.name(), scalar(element, p.name()));
                    } else if (element.isNull()) {
                        r.put(p.name(), null);
                    } else {
                        r.put(p.name(), value(element, p.after(), p.name()));
                    }
                }
            }
            out.add(r);
        }
        pending.addAll(out);
    }

    /** The value at a path below {@code node}, as a string. */
    private static String value(JsonNode node, List<String> segments, String name) {
        JsonNode v = descend(node, segments, name);
        return v == null ? null : scalar(v, name);
    }

    /** Walks object keys; null when a key is missing or a value on the way is null. */
    private static JsonNode descend(JsonNode node, List<String> segments, String name) {
        JsonNode cur = node;
        for (int i = 0; i < segments.size(); i++) {
            if (cur == null || cur.isNull() || cur.isMissingNode()) {
                return null;
            }
            if (!cur.isObject()) {
                throw DeidException.badValue("path '" + name + "': '" + String.join(".", segments.subList(0, i))
                        + "' is a JSON " + kind(cur) + ", not an object");
            }
            cur = cur.get(segments.get(i));
        }
        return cur == null || cur.isNull() || cur.isMissingNode() ? null : cur;
    }

    private static String scalar(JsonNode v, String name) {
        if (v == null || v.isNull() || v.isMissingNode()) {
            return null;
        }
        if (v.isContainerNode()) {
            throw DeidException.badValue("path '" + name + "' leads to a JSON " + kind(v) + ", not a single value");
        }
        if (v.isTextual()) {
            return v.textValue();
        }
        if (v.isBigDecimal()) {
            return v.decimalValue().toPlainString();
        }
        return v.asText();
    }

    /** Records every value path in the record, for columns() and pathsNotSeen(). */
    private void record(JsonNode node, String at) {
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> it = node.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                record(e.getValue(), at.isEmpty() ? e.getKey() : at + "." + e.getKey());
            }
        } else if (node.isArray()) {
            String path = at + "[]";
            for (JsonNode e : node) {
                record(e, path);
            }
        } else {
            notSeen.remove(at);
            if (seen.size() < MAX_SEEN_PATHS) {
                seen.add(at);
            } else if (!seenCapped) {
                seenCapped = true;
                log.warn("deid_json_paths_capped max={} (the manifest's dropped-by-default list is incomplete)", MAX_SEEN_PATHS);
            }
        }
    }

    // ---- helpers ----

    private void skipBom() {
        try {
            reader.mark(1);
            if (reader.read() != '﻿') {
                reader.reset();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("reading the JSON source failed", e);
        }
    }

    /** A parse failure, without Jackson's message: it quotes the source text. */
    private static String invalid(JsonProcessingException e) {
        return (isDuplicate(e) ? "repeats a key" : "isn't valid JSON") + " at column " + e.getLocation().getColumnNr();
    }

    private static boolean isDuplicate(JsonProcessingException e) {
        return e instanceof JsonParseException && e.getOriginalMessage() != null
                && e.getOriginalMessage().startsWith("Duplicate field");
    }

    private static String where(JsonLocation l) {
        return l == null ? "an unknown place" : "line " + l.getLineNr() + ", column " + l.getColumnNr();
    }

    private static String kind(JsonNode n) {
        return n.isArray() ? "array" : n.isObject() ? "object" : n.isTextual() ? "string" : n.isNumber() ? "number"
                : n.isBoolean() ? "boolean" : "null";
    }

    private static String kind(JsonToken t) {
        return switch (t) {
            case START_ARRAY -> "array";
            case VALUE_STRING -> "string";
            case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> "number";
            case VALUE_TRUE, VALUE_FALSE -> "boolean";
            default -> "null";
        };
    }

}
