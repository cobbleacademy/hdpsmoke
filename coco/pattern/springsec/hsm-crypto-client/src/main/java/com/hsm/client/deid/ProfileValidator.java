package com.hsm.client.deid;

import com.hsm.client.deid.ColumnTransform.Method;
import com.hsm.client.deid.transform.GeneralizeTransform;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The rules a profile must pass before any record is read (DEIDENTIFICATION.md
 * sections 4.7, 6.2 and 6.3). Collects every problem, then throws once.
 *
 * <p>The validator can't spot an identifier that isn't tagged with {@code phi}: the
 * dry run and the manifest list passthrough and dropped fields for the reviewer.
 */
public final class ProfileValidator {

    private ProfileValidator() {
    }

    /**
     * @param sourceColumns the source's column names or JSON paths, from the header,
     *                      table metadata or a first-record sample; null to skip the
     *                      checks against the source
     * @throws DeidValidationException listing every problem
     */
    public static void validate(DeidProfile profile, Collection<String> sourceColumns) {
        List<String> problems = problems(profile, sourceColumns);
        if (!problems.isEmpty()) {
            throw new DeidValidationException(problems);
        }
    }

    /** Every problem found; empty when the profile can run. */
    public static List<String> problems(DeidProfile profile, Collection<String> sourceColumns) {
        List<String> problems = new ArrayList<>();
        Set<String> names = new HashSet<>();
        Set<String> outputs = new HashSet<>();
        Set<String> source = sourceColumns == null ? null : Set.copyOf(sourceColumns);

        if (profile.basis() == HipaaBasis.EXPERT_DETERMINATION
                && (profile.expertReport() == null || profile.expertReport().isBlank())) {
            problems.add("expert-determination needs expert-report: <reference to the expert's report>");
        }

        for (DeidProfile.Field f : profile.fields()) {
            String where = "field '" + f.name() + "'";
            if (!names.add(f.name())) {
                problems.add(where + " is listed more than once");
            }
            if (source != null && !source.contains(f.name())) {
                problems.add(where + " is not in the source");
            }

            try {
                TransformRegistry.checkOptions(profile, f);
            } catch (IllegalArgumentException e) {
                problems.add(where + ": " + e.getMessage());
            }

            boolean forbidden = profile.forbiddenFields().matcher(f.name()).find();
            if (forbidden && f.method() != Method.DROP) {
                problems.add(where + " is a never-export field (PIN, CVV, activation or security code): only drop is allowed");
            }

            if (f.decrypt() && f.method() == Method.DROP) {
                problems.add(where + ": decrypt: true with drop has nothing to decrypt for; remove decrypt");
            }

            if (f.method() == Method.TOKENIZE && profile.tokenKeyEdekId() == null) {
                problems.add(where + ": tokenize needs token-key-edek-id in the profile");
            }
            if (f.method() == Method.SURROGATE && profile.surrogateScope() == null) {
                problems.add(where + ": surrogate needs surrogate-scope in the profile");
            }

            GeneralizeTransform generalize = safeGeneralize(f);
            if (generalize != null && generalize.asOfField() != null && source != null
                    && !source.contains(generalize.asOfField())) {
                problems.add(where + ": as-of field '" + generalize.asOfField() + "' is not in the source");
            }

            if (profile.basis() == HipaaBasis.SAFE_HARBOR) {
                safeHarbor(f, generalize, where, problems);
            }

            if (f.method() != Method.DROP) {
                String out = profile.outputName(f);
                if (!outputs.add(out)) {
                    problems.add(where + ": output column '" + out + "' is used more than once");
                }
                if (profile.forbiddenFields().matcher(out).find()) {
                    problems.add(where + ": output column '" + out + "' looks like a never-export field; rename it");
                }
            }
        }

        for (Map.Entry<String, String> r : profile.output().rename().entrySet()) {
            boolean listed = profile.fields().stream()
                    .anyMatch(f -> f.name().equals(r.getKey()) && f.method() != Method.DROP);
            if (!listed) {
                problems.add("output.rename: '" + r.getKey() + "' is not a listed, exported field");
            }
            if (r.getValue() == null || r.getValue().isBlank()) {
                problems.add("output.rename: '" + r.getKey() + "' has an empty output name");
            }
        }
        return problems;
    }

    /** Section 6.2: which methods a Safe Harbor identifier may use. */
    private static void safeHarbor(DeidProfile.Field f, GeneralizeTransform generalize, String where,
                                   List<String> problems) {
        if (f.phi() == null || f.method() == Method.DROP) {
            return;
        }
        PhiCategory.Group group = f.phi().group();
        String category = "phi: " + f.phi().label();
        switch (group) {
            case DIRECT -> problems.add(where + " (" + category + "): safe-harbor allows only drop");
            case UNIQUE_ID -> {
                if (f.method() != Method.SURROGATE) {
                    problems.add(where + " (" + category + "): safe-harbor allows only drop or surrogate"
                            + (f.method() == Method.TOKENIZE
                            ? " (a token is derived from the value, which §164.514(c) forbids)" : ""));
                }
            }
            case BIRTH_DATE -> {
                if (generalize == null || !generalize.isBirthYear()) {
                    problems.add(where + " (" + category + "): safe-harbor allows only drop or generalize to: birth-year"
                            + " (to: year would show ages over 89)");
                } else if (generalize.asOfField() != null) {
                    problems.add(where + " (" + category + "): safe-harbor doesn't allow as-of"
                            + " (an age measured at an older date understates today's age)");
                }
            }
            case DATE -> requireGeneralize(f, generalize, List.of("year"), where, category, problems);
            case ZIP -> requireGeneralize(f, generalize, List.of("zip3"), where, category, problems);
            case AGE -> requireGeneralize(f, generalize, List.of("age-90", "age-band"), where, category, problems);
        }
    }

    private static void requireGeneralize(DeidProfile.Field f, GeneralizeTransform generalize, List<String> allowedTo,
                                          String where, String category, List<String> problems) {
        // A list, not a set: the message names the methods in a fixed order.
        Object to = generalize == null ? null : generalize.describe().get("to");
        if (!allowedTo.contains(String.valueOf(to))) {
            problems.add(where + " (" + category + "): safe-harbor allows only drop or generalize to: "
                    + String.join(" / ", allowedTo));
        }
    }

    private static GeneralizeTransform safeGeneralize(DeidProfile.Field f) {
        try {
            return TransformRegistry.generalize(f);
        } catch (IllegalArgumentException e) {
            return null; // already reported by checkOptions
        }
    }
}
