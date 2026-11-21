package com.hsm.client.deid.transform;

import com.hsm.client.deid.ColumnTransform;
import com.hsm.client.deid.FieldOptions;
import com.hsm.client.deid.FieldStats;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * {@code mask}: hide part of the value (DEIDENTIFICATION.md section 4.2).
 *
 * <pre>
 * card_number=6035712345678901, keep-last: 4                 →  ************8901
 * email=jane.doe@mail.com, keep: domain, keep-first: 1       →  j*******@mail.com
 * phone=312-555-0142, keep-last: 4, preserve: ["-"]          →  ***-***-0142
 * pin, fixed-length: 4                                       →  ****
 * </pre>
 *
 * <ul>
 *   <li>Counts characters as Unicode code points.</li>
 *   <li>A value shorter than {@code keep-first + keep-last} is masked in full.</li>
 *   <li>{@code keep: domain} keeps everything from the last {@code @}; the part before
 *       it is masked apart from the first {@code keep-first} characters. A value with no
 *       {@code @} is masked in full.</li>
 *   <li>{@code fixed-length: N} writes exactly N mask characters for the hidden part,
 *       hiding its real length; it can't be combined with {@code preserve}.</li>
 * </ul>
 *
 * <p><b>Masking is not de-identification:</b> the visible part can still identify
 * someone. The validator allows it on Safe Harbor identifiers only in
 * {@code minimum-necessary} profiles.
 */
public final class MaskTransform implements ColumnTransform {

    record Config(int keepFirst, int keepLast, boolean keepDomain, int maskChar, Set<Integer> preserve,
                  Integer fixedLength) {
    }

    private final Config config;

    private MaskTransform(Config config) {
        this.config = config;
    }

    public static MaskTransform create(FieldOptions options) {
        return new MaskTransform(parse(options));
    }

    static Config parse(FieldOptions o) {
        int keepFirst = o.integer("keep-first", 0, 0, 1000);
        int keepLast = o.integer("keep-last", 0, 0, 1000);
        String keep = o.string("keep", null);
        boolean keepDomain = false;
        if (keep != null) {
            if (!keep.equals("domain")) {
                throw new IllegalArgumentException("option 'keep' supports only 'domain'");
            }
            if (keepLast > 0) {
                throw new IllegalArgumentException("'keep: domain' can't be combined with 'keep-last'");
            }
            keepDomain = true;
        }
        String ch = o.string("char", "*");
        if (ch.codePointCount(0, ch.length()) != 1) {
            throw new IllegalArgumentException("option 'char' must be exactly one character");
        }
        Set<Integer> preserve = new HashSet<>();
        for (String p : o.stringList("preserve")) {
            if (p.codePointCount(0, p.length()) != 1) {
                throw new IllegalArgumentException("each 'preserve' entry must be one character");
            }
            preserve.add(p.codePointAt(0));
        }
        Integer fixed = null;
        if (o.has("fixed-length")) {
            fixed = o.integer("fixed-length", 0, 1, 1000);
            if (!preserve.isEmpty()) {
                throw new IllegalArgumentException("'fixed-length' can't be combined with 'preserve'");
            }
        }
        o.rejectUnknown("mask");
        return new Config(keepFirst, keepLast, keepDomain, ch.codePointAt(0), Set.copyOf(preserve), fixed);
    }

    @Override
    public Method method() {
        return Method.MASK;
    }

    @Override
    public String apply(String value, Map<String, String> record, FieldStats stats) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        if (config.keepDomain()) {
            int at = value.lastIndexOf('@');
            if (at < 0) {
                return maskPart(value.codePoints().toArray(), 0, 0);
            }
            return maskPart(value.substring(0, at).codePoints().toArray(), config.keepFirst(), 0) + value.substring(at);
        }
        return maskPart(value.codePoints().toArray(), config.keepFirst(), config.keepLast());
    }

    private String maskPart(int[] cps, int keepFirst, int keepLast) {
        boolean tooShort = cps.length <= keepFirst + keepLast;
        int first = tooShort ? 0 : keepFirst;
        int last = tooShort ? 0 : keepLast;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < first; i++) {
            sb.appendCodePoint(cps[i]);
        }
        if (config.fixedLength() != null) {
            for (int i = 0; i < config.fixedLength(); i++) {
                sb.appendCodePoint(config.maskChar());
            }
        } else {
            for (int i = first; i < cps.length - last; i++) {
                sb.appendCodePoint(config.preserve().contains(cps[i]) ? cps[i] : config.maskChar());
            }
        }
        for (int i = cps.length - last; i < cps.length; i++) {
            sb.appendCodePoint(cps[i]);
        }
        return sb.toString();
    }

    @Override
    public Map<String, Object> describe() {
        Map<String, Object> d = new LinkedHashMap<>();
        if (config.keepFirst() > 0) {
            d.put("keep-first", config.keepFirst());
        }
        if (config.keepLast() > 0) {
            d.put("keep-last", config.keepLast());
        }
        if (config.keepDomain()) {
            d.put("keep", "domain");
        }
        if (config.fixedLength() != null) {
            d.put("fixed-length", config.fixedLength());
        }
        if (!config.preserve().isEmpty()) {
            d.put("preserve", config.preserve().stream().map(Character::toString).sorted().toList());
        }
        return d;
    }
}
