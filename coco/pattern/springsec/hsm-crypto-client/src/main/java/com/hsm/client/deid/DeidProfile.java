package com.hsm.client.deid;

import com.hsm.client.deid.ColumnTransform.Method;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * A de-identification profile (DEIDENTIFICATION.md section 6): one vendor's fields and
 * methods, and where the CSV goes.
 *
 * <p>Built from the profile document already parsed into maps and lists
 * ({@link #fromMap}), so this module needs no YAML library: the caller (the bulk
 * client) reads the YAML and hashes its bytes for the manifest. {@code fromMap}
 * checks structure only; {@link ProfileValidator} checks the rules.
 */
public record DeidProfile(
        String id,
        int version,
        String vendor,
        HipaaBasis basis,
        String expertReport,
        UUID tokenKeyEdekId,
        String tokenDomain,
        int tokenEpoch,
        String surrogateScope,
        Pattern forbiddenFields,
        List<Field> fields,
        Output output) {

    /**
     * Never-export fields by default: card PINs, CVVs, activation and security codes
     * (section 4.7). Matches a whole name part, so {@code card_pin} and {@code pin}
     * match but {@code shipping_zip} doesn't.
     */
    public static final String DEFAULT_FORBIDDEN_FIELDS =
            "(?i)(^|[_.\\-\\]])(pin|pin_?code|cvv2?|cvc2?|activation(_?code)?|security_?code|passcode)($|[_.\\-\\[])";

    /**
     * One listed field.
     *
     * @param name    source column, or JSON path such as {@code customer.ssn}
     * @param phi     Safe Harbor category, or null for a field that isn't an identifier
     * @param decrypt the source value is HSM ciphertext to decrypt first (section 6.3)
     * @param domain  tokenize/surrogate domain; null to use the profile's {@code token-domain}
     * @param options the method's options, e.g. {@code keep-last}
     */
    public record Field(String name, PhiCategory phi, Method method, boolean decrypt, String domain,
                        Map<String, Object> options) {
        public Field {
            options = options == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(options));
        }
    }

    /**
     * Where and how the CSV is written (section 10.1). Writing is the bulk client's job;
     * the header names ({@link #rename}) are used here.
     */
    public record Output(String file, Map<String, String> rename, char delimiter, boolean crlf, boolean formulaGuard) {
        public Output {
            rename = rename == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(rename));
        }
    }

    private static final Set<String> TOP_LEVEL_KEYS = Set.of(
            "id", "version", "vendor", "hipaa-basis", "expert-report", "token-key-edek-id", "token-domain",
            "token-epoch", "surrogate-scope", "forbidden-fields", "fields", "output");
    private static final Set<String> FIELD_KEYS = Set.of("name", "phi", "method", "decrypt", "domain");
    private static final Set<String> OUTPUT_KEYS = Set.of("file", "rename", "delimiter", "line-ending", "formula-guard");

    public DeidProfile {
        fields = List.copyOf(fields);
    }

    /**
     * The output column name for a source field: renamed, or the source name. A JSON
     * path's {@code []} is left out: {@code orders[].sku} becomes {@code orders.sku}.
     */
    public String outputName(Field field) {
        return output.rename().getOrDefault(field.name(), field.name().replace("[]", ""));
    }

    /**
     * Reads a profile document already parsed into maps and lists (as SnakeYAML or
     * Jackson produce). Collects every structural problem before throwing.
     *
     * @throws DeidValidationException listing every problem found
     */
    public static DeidProfile fromMap(Map<String, ?> doc) {
        List<String> problems = new ArrayList<>();
        unknownKeys(doc.keySet(), TOP_LEVEL_KEYS, "profile", problems);

        String id = requiredString(doc, "id", problems);
        int version = integer(doc.get("version"), "version", problems);
        String vendor = optionalString(doc.get("vendor"));
        HipaaBasis basis = null;
        String basisLabel = requiredString(doc, "hipaa-basis", problems);
        if (basisLabel != null) {
            try {
                basis = HipaaBasis.fromLabel(basisLabel);
            } catch (IllegalArgumentException e) {
                problems.add(e.getMessage());
            }
        }
        UUID tokenKey = null;
        String tokenKeyText = optionalString(doc.get("token-key-edek-id"));
        if (tokenKeyText != null) {
            try {
                tokenKey = UUID.fromString(tokenKeyText);
            } catch (IllegalArgumentException e) {
                problems.add("token-key-edek-id must be a UUID (an edek_id from --deid.init-key)");
            }
        }
        int tokenEpoch = doc.get("token-epoch") == null ? 1 : integer(doc.get("token-epoch"), "token-epoch", problems);

        Pattern forbidden = null;
        String forbiddenText = optionalString(doc.get("forbidden-fields"));
        try {
            forbidden = Pattern.compile(forbiddenText != null ? forbiddenText : DEFAULT_FORBIDDEN_FIELDS);
        } catch (PatternSyntaxException e) {
            problems.add("forbidden-fields is not a valid regular expression");
        }

        List<Field> fields = parseFields(doc.get("fields"), problems);
        Output output = parseOutput(doc.get("output"), problems);

        if (!problems.isEmpty()) {
            throw new DeidValidationException(problems);
        }
        return new DeidProfile(id, version, vendor, basis, optionalString(doc.get("expert-report")), tokenKey,
                optionalString(doc.get("token-domain")), tokenEpoch, optionalString(doc.get("surrogate-scope")),
                forbidden, fields, output);
    }

    private static List<Field> parseFields(Object raw, List<String> problems) {
        List<Field> fields = new ArrayList<>();
        if (!(raw instanceof List<?> list) || list.isEmpty()) {
            problems.add("fields must be a non-empty list");
            return fields;
        }
        for (int i = 0; i < list.size(); i++) {
            if (!(list.get(i) instanceof Map<?, ?> entry)) {
                problems.add("fields[" + i + "] must be a map with name and method");
                continue;
            }
            String name = optionalString(entry.get("name"));
            String where = name != null ? "field '" + name + "'" : "fields[" + i + "]";
            if (name == null) {
                problems.add(where + ": name is required");
                continue;
            }
            Method method = null;
            String methodLabel = optionalString(entry.get("method"));
            if (methodLabel == null) {
                problems.add(where + ": method is required");
            } else {
                try {
                    method = Method.fromLabel(methodLabel);
                } catch (IllegalArgumentException e) {
                    problems.add(where + ": " + e.getMessage());
                }
            }
            PhiCategory phi = null;
            String phiLabel = optionalString(entry.get("phi"));
            if (phiLabel != null) {
                try {
                    phi = PhiCategory.fromLabel(phiLabel);
                } catch (IllegalArgumentException e) {
                    problems.add(where + ": " + e.getMessage());
                }
            }
            boolean decrypt = false;
            Object decryptRaw = entry.get("decrypt");
            if (decryptRaw instanceof Boolean b) {
                decrypt = b;
            } else if (decryptRaw != null) {
                problems.add(where + ": decrypt must be true or false");
            }
            Map<String, Object> options = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : entry.entrySet()) {
                String key = String.valueOf(e.getKey());
                if (!FIELD_KEYS.contains(key)) {
                    options.put(key, e.getValue());
                }
            }
            if (method != null) {
                fields.add(new Field(name, phi, method, decrypt, optionalString(entry.get("domain")), options));
            }
        }
        return fields;
    }

    private static Output parseOutput(Object raw, List<String> problems) {
        if (!(raw instanceof Map<?, ?> out)) {
            problems.add("output must be a map with at least file");
            return null;
        }
        Set<String> keys = new TreeSet<>();
        out.keySet().forEach(k -> keys.add(String.valueOf(k)));
        unknownKeys(keys, OUTPUT_KEYS, "output", problems);

        String file = optionalString(out.get("file"));
        if (file == null) {
            problems.add("output.file is required");
        }
        Map<String, String> rename = new LinkedHashMap<>();
        Object renameRaw = out.get("rename");
        if (renameRaw instanceof Map<?, ?> r) {
            r.forEach((k, v) -> rename.put(String.valueOf(k), String.valueOf(v)));
        } else if (renameRaw != null) {
            problems.add("output.rename must be a map of source name to output name");
        }
        char delimiter = ',';
        String delimiterText = optionalString(out.get("delimiter"));
        if (delimiterText != null) {
            if (delimiterText.length() != 1 || delimiterText.equals("\"") || delimiterText.equals("\n")
                    || delimiterText.equals("\r")) {
                problems.add("output.delimiter must be one character other than a quote or line break");
            } else {
                delimiter = delimiterText.charAt(0);
            }
        }
        boolean crlf = false;
        String lineEnding = optionalString(out.get("line-ending"));
        if (lineEnding != null) {
            switch (lineEnding) {
                case "lf" -> crlf = false;
                case "crlf" -> crlf = true;
                default -> problems.add("output.line-ending must be lf or crlf");
            }
        }
        boolean formulaGuard = true;
        Object guard = out.get("formula-guard");
        if (guard instanceof Boolean b) {
            formulaGuard = b;
        } else if (guard != null) {
            problems.add("output.formula-guard must be true or false");
        }
        return new Output(file, rename, delimiter, crlf, formulaGuard);
    }

    private static void unknownKeys(Set<?> keys, Set<String> allowed, String where, List<String> problems) {
        Set<String> unknown = new TreeSet<>();
        for (Object k : keys) {
            if (!allowed.contains(String.valueOf(k))) {
                unknown.add(String.valueOf(k));
            }
        }
        if (!unknown.isEmpty()) {
            problems.add("unknown key(s) in " + where + ": " + String.join(", ", unknown));
        }
    }

    private static String requiredString(Map<String, ?> doc, String key, List<String> problems) {
        String v = optionalString(doc.get(key));
        if (v == null) {
            problems.add(key + " is required");
        }
        return v;
    }

    private static String optionalString(Object v) {
        if (v == null) {
            return null;
        }
        String s = String.valueOf(v).trim();
        return s.isEmpty() ? null : s;
    }

    private static int integer(Object v, String key, List<String> problems) {
        if (v instanceof Integer n) {
            return n;
        }
        if (v != null) {
            try {
                return Integer.parseInt(String.valueOf(v).trim());
            } catch (NumberFormatException ignored) {
                // reported below
            }
        }
        problems.add(key + " is required and must be a whole number");
        return 0;
    }
}
