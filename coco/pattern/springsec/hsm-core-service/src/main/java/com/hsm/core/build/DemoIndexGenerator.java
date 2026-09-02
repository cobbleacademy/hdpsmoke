package com.hsm.core.build;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Injects docs/architecture-diagram.svg and docs/sequence-diagram.svg into the
 * live demo's index.html template at build time, so those two files stay the
 * single, hand-edited source of truth -- the live demo's diagrams are always a
 * fresh copy of them, never a manually-pasted-and-forgotten one.
 *
 * <p>Why generate an inline copy at all, instead of just having index.html
 * {@code <object>}-reference docs/*.svg directly: a separate Python-based
 * project reads index.html's raw source (not a rendered DOM) to drive
 * DEMO-LIVE, so the SVG markup has to stay textually present in the file it
 * reads -- changing that delivery shape would break that consumer.
 *
 * <p>Ported from an earlier Python script (generate_demo_index.py) to pure
 * Java for two reasons found together: (1) the Docker build stage for this
 * module ({@code maven:...-eclipse-temurin-21}) has no Python interpreter at
 * all, so a Python-based step simply cannot run inside that container;
 * (2) the source SVGs used to live in a top-level {@code docs/} directory
 * one level *above* {@code java/} -- outside that Dockerfile's own build
 * context ({@code java/}), which Docker's COPY can never reach regardless of
 * which language reads it. Both are fixed together here: the SVGs now live
 * in {@code java/docs/} (inside the build context, and inside this
 * generator's own reactor), and this class needs nothing beyond what's
 * already being {@code COPY}'d into the image ({@code hsm-core-service/src},
 * which now includes this class) -- no interpreter, no extra COPY beyond
 * {@code docs/} itself.
 *
 * <p>Invoked via exec-maven-plugin's {@code java} goal (in-process, no
 * subprocess spawned at all -- lighter weight than shelling out to an
 * external interpreter), bound to the {@code prepare-package} phase:
 * deliberately after both {@code process-resources} (the default resource
 * copy, which places the checked-in template -- still containing
 * {@code @@...@@} markers -- at the output path this class overwrites) and
 * {@code compile} (so this class's own bytecode already exists to be run at
 * all) have already completed, so there is no phase-ordering ambiguity to
 * rely on: this always runs strictly after both, not "probably after,
 * because explicit executions run after implicit default bindings for the
 * same phase" (the fragile ordering the Python version's process-resources
 * binding depended on).
 *
 * <p>Usage: {@code DemoIndexGenerator <docs-dir> <template-path> <output-path>}
 */
public final class DemoIndexGenerator {

    private static final String ARCH_MARKER = "@@ARCHITECTURE_DIAGRAM_SVG@@";
    private static final String SEQ_HEADER_MARKER = "@@SEQUENCE_DIAGRAM_HEADER@@";
    private static final String SEQ_BODY_MARKER = "@@SEQUENCE_DIAGRAM_BODY@@";
    private static final String HEADER_END_MARKER = "<!-- LIVE_DEMO_HEADER_END -->";
    private static final String BODY_OPEN_TAG = "<g transform=\"translate(0,80)\">";

    private DemoIndexGenerator() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "usage: DemoIndexGenerator <docs-dir> <template-path> <output-path>");
        }
        Path docsDir = Path.of(args[0]);
        Path templatePath = Path.of(args[1]);
        Path outputPath = Path.of(args[2]);

        String archSvg = readTextOrFail(docsDir.resolve("architecture-diagram.svg"), "docs/architecture-diagram.svg").strip();
        String seqSvg = readTextOrFail(docsDir.resolve("sequence-diagram.svg"), "docs/sequence-diagram.svg");

        String seqHeader = extractSequenceHeader(seqSvg);
        String seqBody = extractSequenceBody(seqSvg);

        String template = readTextOrFail(templatePath, "index.html template");
        for (String marker : new String[]{ARCH_MARKER, SEQ_HEADER_MARKER, SEQ_BODY_MARKER}) {
            long count = countOccurrences(template, marker);
            if (count != 1) {
                throw new IllegalStateException("expected exactly one " + marker + " in " + templatePath
                        + ", found " + count + " -- was the template edited without updating this generator?");
            }
        }

        String generated = template
                .replace(ARCH_MARKER, archSvg)
                .replace(SEQ_HEADER_MARKER, seqHeader)
                .replace(SEQ_BODY_MARKER, seqBody);

        Files.createDirectories(outputPath.getParent());
        Files.writeString(outputPath, generated, StandardCharsets.UTF_8);
        System.out.println("DemoIndexGenerator: wrote " + outputPath + " (" + generated.length()
                + " chars) from docs/architecture-diagram.svg + docs/sequence-diagram.svg");
    }

    private static String extractSequenceHeader(String seqSvg) {
        Pattern pattern = Pattern.compile(
                "<!-- LIVE_DEMO_HEADER_START.*?-->(.*?)" + Pattern.quote(HEADER_END_MARKER), Pattern.DOTALL);
        Matcher matcher = pattern.matcher(seqSvg);
        if (!matcher.find()) {
            throw new IllegalStateException(
                    "LIVE_DEMO_HEADER_START/END markers not found in docs/sequence-diagram.svg -- "
                            + "did the header section get restructured? Update this generator's extraction to match.");
        }
        return matcher.group(1).strip();
    }

    /** Searches only after LIVE_DEMO_HEADER_END, not from the start of the file --
     * this exact open-tag string also appears earlier, inside the
     * LIVE_DEMO_HEADER_START/END explanatory comment itself (which describes this
     * very tag), so searching from position 0 would match that comment instead of
     * the real tag and pull the header content into the body a second time.
     * Anchoring past the header marker makes that class of collision impossible
     * regardless of what the comment text says. */
    private static String extractSequenceBody(String seqSvg) {
        int headerEnd = seqSvg.indexOf(HEADER_END_MARKER);
        if (headerEnd == -1) {
            throw new IllegalStateException("LIVE_DEMO_HEADER_END marker not found in docs/sequence-diagram.svg");
        }
        int searchFrom = headerEnd + HEADER_END_MARKER.length();

        int start = seqSvg.indexOf(BODY_OPEN_TAG, searchFrom);
        if (start == -1) {
            throw new IllegalStateException(
                    "'" + BODY_OPEN_TAG + "' not found after LIVE_DEMO_HEADER_END in docs/sequence-diagram.svg -- "
                            + "did the body's wrapping group get restructured? Update this generator's extraction to match.");
        }
        start += BODY_OPEN_TAG.length();
        int end = seqSvg.lastIndexOf("</g>");
        if (end == -1 || end < start) {
            throw new IllegalStateException("no closing </g> found after the body's opening group in docs/sequence-diagram.svg");
        }
        return seqSvg.substring(start, end).strip();
    }

    private static String readTextOrFail(Path path, String what) throws IOException {
        if (!Files.isRegularFile(path)) {
            throw new IllegalStateException(what + " not found at " + path.toAbsolutePath());
        }
        return Files.readString(path, StandardCharsets.UTF_8);
    }

    private static long countOccurrences(String haystack, String needle) {
        long count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) != -1) {
            count++;
            idx += needle.length();
        }
        return count;
    }
}
