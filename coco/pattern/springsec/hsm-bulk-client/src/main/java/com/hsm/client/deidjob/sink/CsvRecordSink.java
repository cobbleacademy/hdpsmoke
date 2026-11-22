package com.hsm.client.deidjob.sink;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

/**
 * Writes the vendor CSV (DEIDENTIFICATION.md section 10.1): UTF-8 with no BOM, RFC 4180
 * quoting, a header row, {@code \n} (or {@code \r\n}) line endings.
 *
 * <p>The file is written as {@code <name>.part} and renamed by {@link #commit()} when
 * complete, so a transfer process watching the folder never picks up a half-written
 * file. {@link #abort()} deletes the part file. The SHA-256 of the bytes written is
 * computed on the way.
 *
 * <p>Formula guard (on by default): a value starting with {@code = + - @}, tab or
 * carriage return gets a leading {@code '}, so a spreadsheet doesn't run it.
 */
public final class CsvRecordSink implements AutoCloseable {

    private final Path target;
    private final Path part;
    private final char delimiter;
    private final String lineEnding;
    private final boolean formulaGuard;
    private final MessageDigest digest;
    private final Writer writer;
    private long rows;
    private long guarded;
    private boolean finished;
    private String sha256;

    public CsvRecordSink(Path target, char delimiter, boolean crlf, boolean formulaGuard) {
        if (delimiter == '"' || delimiter == '\n' || delimiter == '\r') {
            throw new IllegalArgumentException("the output delimiter can't be a quote or a line break");
        }
        this.target = target;
        this.part = target.resolveSibling(target.getFileName() + ".part");
        this.delimiter = delimiter;
        this.lineEnding = crlf ? "\r\n" : "\n";
        this.formulaGuard = formulaGuard;
        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        try {
            if (target.getParent() != null) {
                Files.createDirectories(target.getParent());
            }
            if (Files.exists(target)) {
                throw new IllegalStateException("output file " + target.getFileName() + " already exists; not overwriting it");
            }
            OutputStream out = new DigestOutputStream(Files.newOutputStream(part), digest);
            this.writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8), 64 * 1024);
        } catch (IOException e) {
            throw new UncheckedIOException("can't create " + part, e);
        }
    }

    public void writeHeader(List<String> header) {
        writeLine(header, false);
    }

    public void writeRows(List<List<String>> page) {
        for (List<String> row : page) {
            writeLine(row, formulaGuard);
            rows++;
        }
    }

    private void writeLine(List<String> values, boolean guard) {
        try {
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) {
                    writer.write(delimiter);
                }
                writer.write(escape(values.get(i), guard));
            }
            writer.write(lineEnding);
        } catch (IOException e) {
            throw new UncheckedIOException("writing " + part.getFileName() + " failed", e);
        }
    }

    String escape(String value, boolean guard) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        if (guard && needsGuard(value.charAt(0))) {
            value = "'" + value;
            guarded++;
        }
        boolean quote = false;
        for (int i = 0; i < value.length() && !quote; i++) {
            char c = value.charAt(i);
            quote = c == delimiter || c == '"' || c == '\n' || c == '\r';
        }
        return quote ? '"' + value.replace("\"", "\"\"") + '"' : value;
    }

    private static boolean needsGuard(char c) {
        return c == '=' || c == '+' || c == '-' || c == '@' || c == '\t' || c == '\r';
    }

    /** Flushes, closes and renames {@code .part} to the final name. */
    public void commit() {
        if (finished) {
            throw new IllegalStateException("already finished");
        }
        try {
            writer.close();
            sha256 = HexFormat.of().formatHex(digest.digest());
            try {
                Files.move(part, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(part, target);
            }
            finished = true;
        } catch (IOException e) {
            abort();
            throw new UncheckedIOException("finishing " + target.getFileName() + " failed", e);
        }
    }

    /** Closes and deletes the part file. Safe to call more than once, and after a failed commit. */
    public void abort() {
        if (finished) {
            return;
        }
        finished = true;
        try {
            writer.close();
        } catch (IOException ignored) {
            // deleting it anyway
        }
        try {
            Files.deleteIfExists(part);
        } catch (IOException e) {
            throw new UncheckedIOException("can't delete " + part, e);
        }
    }

    /** Aborts unless committed. */
    @Override
    public void close() {
        abort();
    }

    public long rows() {
        return rows;
    }

    /** Values that got a leading {@code '} from the formula guard. */
    public long guarded() {
        return guarded;
    }

    /** Hex SHA-256 of the committed file. */
    public String sha256() {
        if (sha256 == null) {
            throw new IllegalStateException("not committed");
        }
        return sha256;
    }

    public Path target() {
        return target;
    }
}
