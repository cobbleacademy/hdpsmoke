package com.hsm.client.deid.transform;

import com.hsm.client.deid.ColumnTransform;
import com.hsm.client.deid.DeidException;
import com.hsm.client.deid.FieldOptions;
import com.hsm.client.deid.FieldStats;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@code generalize}: make the value less precise (DEIDENTIFICATION.md section 4.3).
 *
 * <pre>
 * to: year         issued=2026-10-14  →  2026
 * to: birth-year   dob=1958-03-12     →  1958
 * to: birth-year   dob=1933-07-30     →  &lt;=1936    (run in 2026: 90 or over)
 * to: zip3         zip=60614          →  606        (restricted ZIP3s → 000)
 * to: age-90       age=92             →  90+
 * to: age-band     age=43             →  40-49      (width: 10)
 * </pre>
 *
 * <p><b>{@code birth-year}</b> caps ages over 89, as Safe Harbor requires for every date
 * element that shows such an age, the year included. Cut-off year = the run's year
 * (UTC) − 90, or the year of the {@code as-of} field's date. It compares years only, so
 * someone born in the cut-off year who is still 89 is capped too.
 *
 * <p>A value that can't be parsed fails the run.
 */
public final class GeneralizeTransform implements ColumnTransform {

    /** Ages from this one up are written as {@code 90+}. */
    static final int AGE_CAP = 90;

    enum To {
        YEAR, BIRTH_YEAR, ZIP3, AGE_90, AGE_BAND;

        String label() {
            return name().toLowerCase(Locale.ROOT).replace('_', '-');
        }

        static To fromLabel(String label) {
            for (To t : values()) {
                if (t.label().equals(label)) {
                    return t;
                }
            }
            throw new IllegalArgumentException("option 'to' must be one of year, birth-year, zip3, age-90, age-band");
        }
    }

    record Config(To to, DateTimeFormatter inputFormat, String inputFormatText, int width, String over89As,
                  String asOf) {
    }

    private static final Pattern ZIP = Pattern.compile("(\\d{5})(?:-?(\\d{4}))?");

    private final Config config;
    private final Clock clock;
    private final Set<String> restrictedZip3;

    private GeneralizeTransform(Config config, Clock clock, Set<String> restrictedZip3) {
        this.config = config;
        this.clock = clock;
        this.restrictedZip3 = restrictedZip3;
    }

    public static GeneralizeTransform create(FieldOptions options, Clock clock, Set<String> restrictedZip3) {
        return new GeneralizeTransform(parse(options), clock, restrictedZip3);
    }

    static Config parse(FieldOptions o) {
        String toText = o.string("to", null);
        if (toText == null) {
            throw new IllegalArgumentException("option 'to' is required (year, birth-year, zip3, age-90 or age-band)");
        }
        To to = To.fromLabel(toText);
        DateTimeFormatter format = null;
        String formatText = null;
        int width = 10;
        String over89As = null;
        String asOf = null;
        switch (to) {
            case YEAR, BIRTH_YEAR -> {
                formatText = o.string("input-format", null);
                if (formatText != null) {
                    try {
                        format = DateTimeFormatter.ofPattern(formatText, Locale.ROOT);
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("option 'input-format' is not a valid date pattern");
                    }
                }
                if (to == To.BIRTH_YEAR) {
                    over89As = o.string("over-89-as", null);
                    asOf = o.string("as-of", null);
                }
            }
            case AGE_BAND -> width = o.integer("width", 10, 2, 50);
            case ZIP3, AGE_90 -> {
                // no options
            }
        }
        o.rejectUnknown("generalize to: " + to.label());
        return new Config(to, format, formatText, width, over89As, asOf);
    }

    /** The {@code as-of} field name, or null; the validator checks it exists. */
    public String asOfField() {
        return config.asOf();
    }

    /** True for {@code to: birth-year}. */
    public boolean isBirthYear() {
        return config.to() == To.BIRTH_YEAR;
    }

    @Override
    public Method method() {
        return Method.GENERALIZE;
    }

    @Override
    public String apply(String value, Map<String, String> record, FieldStats stats) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        return switch (config.to()) {
            case YEAR -> Integer.toString(year(value));
            case BIRTH_YEAR -> birthYear(value, record, stats);
            case ZIP3 -> zip3(value, stats);
            case AGE_90 -> age90(value, stats);
            case AGE_BAND -> ageBand(value, stats);
        };
    }

    private String birthYear(String value, Map<String, String> record, FieldStats stats) {
        int born = year(value);
        int cutoff = asOfYear(record) - AGE_CAP;
        if (born <= cutoff) {
            stats.increment("capped");
            return config.over89As() != null ? config.over89As() : "<=" + cutoff;
        }
        return Integer.toString(born);
    }

    private int asOfYear(Map<String, String> record) {
        if (config.asOf() != null) {
            String asOf = record == null ? null : record.get(config.asOf());
            if (asOf != null && !asOf.isEmpty()) {
                try {
                    return year(asOf);
                } catch (DeidException e) {
                    throw DeidException.badValue("the as-of field '" + config.asOf() + "' is not a date");
                }
            }
        }
        // No as-of date: the run date, which hides the most (ages only go up).
        return runYear();
    }

    private int runYear() {
        return Instant.now(clock).atOffset(ZoneOffset.UTC).getYear();
    }

    private int year(String value) {
        String v = value.trim();
        try {
            if (config.inputFormat() != null) {
                TemporalAccessor parsed = config.inputFormat().parse(v);
                return parsed.get(ChronoField.YEAR);
            }
        } catch (java.time.DateTimeException e) {
            throw DeidException.badValue("not a date in the input-format");
        }
        try {
            return LocalDate.parse(v).getYear();
        } catch (DateTimeParseException ignored) {
            // try the next form
        }
        try {
            return OffsetDateTime.parse(v).getYear();
        } catch (DateTimeParseException ignored) {
            // try the next form
        }
        try {
            return LocalDateTime.parse(v).getYear();
        } catch (DateTimeParseException ignored) {
            // try the next form
        }
        try {
            return Instant.parse(v).atOffset(ZoneOffset.UTC).getYear();
        } catch (DateTimeParseException ignored) {
            throw DeidException.badValue("not an ISO date or date-time; set input-format");
        }
    }

    private String zip3(String value, FieldStats stats) {
        Matcher m = ZIP.matcher(value.trim());
        if (!m.matches()) {
            throw DeidException.badValue("not a 5-digit ZIP or ZIP+4");
        }
        String zip3 = m.group(1).substring(0, 3);
        if (restrictedZip3.contains(zip3)) {
            stats.increment("restricted");
            return "000";
        }
        return zip3;
    }

    private static int age(String value) {
        try {
            int age = Integer.parseInt(value.trim());
            if (age < 0 || age > 150) {
                throw DeidException.badValue("age out of range 0-150");
            }
            return age;
        } catch (NumberFormatException e) {
            throw DeidException.badValue("not a whole-number age");
        }
    }

    private String age90(String value, FieldStats stats) {
        int age = age(value);
        if (age >= AGE_CAP) {
            stats.increment("capped");
            return "90+";
        }
        return Integer.toString(age);
    }

    private String ageBand(String value, FieldStats stats) {
        int age = age(value);
        if (age >= AGE_CAP) {
            stats.increment("capped");
            return "90+";
        }
        int low = (age / config.width()) * config.width();
        int high = Math.min(low + config.width() - 1, AGE_CAP - 1);
        return low + "-" + high;
    }

    @Override
    public Map<String, Object> describe() {
        Map<String, Object> d = new LinkedHashMap<>();
        d.put("to", config.to().label());
        if (config.inputFormatText() != null) {
            d.put("input-format", config.inputFormatText());
        }
        if (config.to() == To.AGE_BAND) {
            d.put("width", config.width());
        }
        if (config.to() == To.BIRTH_YEAR) {
            if (config.asOf() != null) {
                d.put("as-of", config.asOf());
            } else {
                d.put("cutoff_year", runYear() - AGE_CAP);
            }
            if (config.over89As() != null) {
                d.put("over-89-as", config.over89As());
            }
        }
        return d;
    }
}
