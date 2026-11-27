package com.hsm.edekmigrate;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * The tool's settings, from a properties file. Secrets never sit in the file: it names
 * the environment variables that hold them ({@code *-env} keys).
 *
 * <p>Every key, whether it's required, its default and what it means:
 * {@code src/main/resources/migrate.properties.template}, which
 * {@code java -jar hsm-edek-migrate.jar template} prints.
 */
public record MigrateConfig(
        Side source,
        Side target,
        String sourceLegacyKekName,
        String targetAccessSchema,
        String stagingSchema,
        Map<String, String> kekMap,
        int pageSize,
        int maxConsecutiveFailures) {

    /** Schema names go into SQL text, so only plain lower-case identifiers are accepted. */
    private static final Pattern IDENTIFIER = Pattern.compile("[a-z_][a-z0-9_]{0,62}");
    /** Key Vault key names: 1-127 of letters, digits and hyphens. */
    private static final Pattern KEK_NAME = Pattern.compile("[A-Za-z0-9-]{1,127}");

    /** One side of the move: its database and its vault. */
    public record Side(String name, String jdbcUrl, String user, String password, String cryptoSchema,
                       String vaultUrl, CredentialConfig credential) {
        @Override
        public String toString() {
            return "Side[" + name + ", " + jdbcUrl + ", user=" + user + ", password=<redacted>, schema="
                    + cryptoSchema + ", vault=" + vaultUrl + ", " + credential + "]";
        }
    }

    /**
     * How a side signs in to its vault. mode: client-secret, client-certificate,
     * workload-identity, managed-identity or default.
     */
    public record CredentialConfig(String mode, String tenantId, String clientId, String clientSecret,
                                   String certificatePath, String tokenFile) {
        @Override
        public String toString() {
            return "Credential[" + mode + ", tenant=" + tenantId + ", client=" + clientId
                    + (clientSecret == null ? "" : ", secret=<redacted>") + "]";
        }
    }

    public static MigrateConfig load(Path file, Map<String, String> env) throws IOException {
        Properties p = new Properties();
        try (Reader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            p.load(r);
        }
        return from(p, env);
    }

    public static MigrateConfig from(Properties p, Map<String, String> env) {
        List<String> problems = new ArrayList<>();
        Side source = side("source", p, env, problems);
        Side target = side("target", p, env, problems);

        String legacy = required(p, "source.legacy-kek-name", problems);
        if (legacy != null && !KEK_NAME.matcher(legacy).matches()) {
            problems.add("source.legacy-kek-name is not a valid key name");
        }
        String access = identifier(p, "target.access-schema", null, problems);
        String staging = identifier(p, "target.staging-schema", "edek_migrate", problems);
        if (staging != null && target != null && staging.equals(target.cryptoSchema())) {
            problems.add("target.staging-schema must differ from target.crypto-schema");
        }

        Map<String, String> kekMap = new TreeMap<>();
        for (String key : p.stringPropertyNames()) {
            if (key.startsWith("kek-map.")) {
                String from = key.substring("kek-map.".length());
                String to = p.getProperty(key).trim();
                if (!KEK_NAME.matcher(from).matches() || !KEK_NAME.matcher(to).matches()) {
                    problems.add("kek-map entry '" + key + "' is not a valid key name");
                }
                kekMap.put(from, to);
            }
        }
        if (kekMap.isEmpty()) {
            problems.add("no kek-map.<source KEK>=<target KEK> entries");
        }

        int pageSize = positiveInt(p, "page-size", 500, problems);
        int maxFailures = positiveInt(p, "max-consecutive-failures", 10, problems);

        if (!problems.isEmpty()) {
            throw new ConfigException(problems);
        }
        return new MigrateConfig(source, target, legacy, access, staging, Collections.unmodifiableMap(kekMap),
                pageSize, maxFailures);
    }

    /** The target KEK for a source KEK, or null if the map has none. */
    public String targetKekFor(String sourceKekName) {
        return kekMap.get(sourceKekName);
    }

    private static Side side(String name, Properties p, Map<String, String> env, List<String> problems) {
        String url = required(p, name + ".jdbc-url", problems);
        String user = required(p, name + ".user", problems);
        String password = fromEnv(p, name + ".password-env", env, problems);
        String crypto = identifier(p, name + ".crypto-schema", null, problems);
        String vault = required(p, name + ".vault-url", problems);
        if (vault != null && !vault.startsWith("https://")) {
            problems.add(name + ".vault-url must start with https://");
        }
        CredentialConfig credential = credential(name, p, env, problems);
        return new Side(name, url, user, password, crypto, vault, credential);
    }

    private static CredentialConfig credential(String side, Properties p, Map<String, String> env, List<String> problems) {
        String mode = value(p, side + ".credential");
        if (mode == null) {
            problems.add(side + ".credential is required (client-secret, client-certificate, workload-identity, "
                    + "managed-identity or default)");
            return null;
        }
        String tenant = value(p, side + ".tenant-id");
        String client = value(p, side + ".client-id");
        String secret = null;
        String cert = null;
        String tokenFile = null;
        switch (mode) {
            case "client-secret" -> {
                requireAll(side, mode, problems, tenant, client);
                secret = fromEnv(p, side + ".client-secret-env", env, problems);
            }
            case "client-certificate" -> {
                requireAll(side, mode, problems, tenant, client);
                cert = required(p, side + ".client-certificate-path", problems);
            }
            case "workload-identity" -> {
                requireAll(side, mode, problems, tenant, client);
                tokenFile = value(p, side + ".token-file");
                if (tokenFile == null) {
                    tokenFile = env.getOrDefault("AZURE_FEDERATED_TOKEN_FILE",
                            "/var/run/secrets/azure/tokens/azure-identity-token");
                }
            }
            case "managed-identity", "default" -> {
                // client-id optional: picks a user-assigned identity
            }
            default -> problems.add(side + ".credential '" + mode + "' is not one of client-secret, "
                    + "client-certificate, workload-identity, managed-identity, default");
        }
        return new CredentialConfig(mode, tenant, client, secret, cert, tokenFile);
    }

    private static void requireAll(String side, String mode, List<String> problems, String tenant, String client) {
        if (tenant == null || client == null) {
            problems.add(side + ".credential=" + mode + " needs " + side + ".tenant-id and " + side + ".client-id");
        }
    }

    private static String value(Properties p, String key) {
        String v = p.getProperty(key);
        return v == null || v.isBlank() ? null : v.trim();
    }

    private static String required(Properties p, String key, List<String> problems) {
        String v = value(p, key);
        if (v == null) {
            problems.add(key + " is required");
        }
        return v;
    }

    private static String fromEnv(Properties p, String key, Map<String, String> env, List<String> problems) {
        String var = required(p, key, problems);
        if (var == null) {
            return null;
        }
        String v = env.get(var);
        if (v == null || v.isEmpty()) {
            problems.add(key + " names " + var + ", which isn't set");
        }
        return v;
    }

    private static String identifier(Properties p, String key, String dflt, List<String> problems) {
        String v = value(p, key);
        if (v == null) {
            v = dflt;
        }
        if (v == null) {
            problems.add(key + " is required");
        } else if (!IDENTIFIER.matcher(v).matches()) {
            problems.add(key + " must be a lower-case identifier ([a-z_][a-z0-9_]*)");
        }
        return v;
    }

    private static int positiveInt(Properties p, String key, int dflt, List<String> problems) {
        String v = value(p, key);
        if (v == null) {
            return dflt;
        }
        try {
            int n = Integer.parseInt(v);
            if (n > 0) {
                return n;
            }
        } catch (NumberFormatException ignored) {
            // reported below
        }
        problems.add(key + " must be a positive whole number");
        return dflt;
    }

    /** Every problem in the file at once, not the first one only. */
    public static final class ConfigException extends RuntimeException {
        private final List<String> problems;

        ConfigException(List<String> problems) {
            super(String.join("; ", problems));
            this.problems = List.copyOf(problems);
        }

        public List<String> problems() {
            return problems;
        }
    }
}
