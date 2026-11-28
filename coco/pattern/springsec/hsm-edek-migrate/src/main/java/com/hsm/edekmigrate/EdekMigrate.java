package com.hsm.edekmigrate;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Moves hsm-core's EDEKs from one tenant (A) to another (B) by re-wrapping each DEK:
 * unwrap with A's KEK, wrap with B's, check, then promote into B's edek_records. Design
 * and runbook: java/docs/EDEK_TENANT_MIGRATION.md.
 *
 * <pre>
 * java -jar hsm-edek-migrate.jar &lt;command&gt; --config migrate.properties [--kek NAME[,NAME]] [--app APP_ID[,APP_ID]] [--ids FILE] [--yes] [--no-probe]
 * java -jar hsm-edek-migrate.jar template &gt; migrate.properties
 * </pre>
 *
 * <p>Every setting is explained in {@code src/main/resources/migrate.properties.template}.
 */
public final class EdekMigrate {

    private static final Set<String> COMMANDS = Set.of("analyze", "stage", "wrap", "verify", "report", "promote",
            "reset");

    private EdekMigrate() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.getenv(), System.out, System.err));
    }

    static int run(String[] args, Map<String, String> env, PrintStream out, PrintStream err) {
        if (args.length == 1 && args[0].equals("template")) {
            return template(out, err);
        }
        if (args.length == 1 && Set.of("help", "--help", "-h").contains(args[0])) {
            out.println(usage());
            out.println(HELP);
            return Commands.OK;
        }
        Args a;
        try {
            a = Args.parse(args);
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            err.println(usage());
            return Commands.USAGE;
        }
        MigrateConfig config;
        try {
            if (!Files.isRegularFile(a.config())) {
                err.println("config file not found: " + a.config());
                return Commands.USAGE;
            }
            config = MigrateConfig.load(a.config(), env);
        } catch (MigrateConfig.ConfigException e) {
            e.problems().forEach(p -> err.println("config: " + p));
            return Commands.USAGE;
        } catch (Exception e) {
            err.println("config: can't read " + a.config() + ": " + Commands.errorType(e));
            return Commands.USAGE;
        }

        Connection source = null;
        Connection target = null;
        try {
            try {
                source = connect(config.source());
            } catch (SQLException e) {
                err.println(Errors.connectFailed("source", config.source(), e));
                return Commands.STOPPED;
            }
            try {
                target = connect(config.target());
            } catch (SQLException e) {
                err.println(Errors.connectFailed("target", config.target(), e));
                return Commands.STOPPED;
            }
            return command(a, config, source, target, out, err);
        } finally {
            close(target);
            close(source);
        }
    }

    private static int command(Args a, MigrateConfig config, Connection source, Connection target, PrintStream out,
                               PrintStream err) {
        Commands c = null;
        try {
            c = new Commands(config, source, target, AzureKekVault.of(config.source()),
                    AzureKekVault.of(config.target()), out);
            return switch (a.command()) {
                case "analyze" -> c.analyze(a.scope(), a.probe());
                case "stage" -> c.stage(a.scope());
                case "wrap" -> c.wrap(a.scope());
                case "verify" -> c.verify(a.scope());
                case "report" -> c.report(a.scope());
                case "promote" -> c.promote(a.scope());
                case "reset" -> c.reset(a.scope(), a.yes());
                default -> Commands.USAGE;
            };
        } catch (SQLException e) {
            String side = c == null ? "target" : c.stepSide();
            err.println(a.command() + " stopped: " + (c == null ? "setting up" : c.step()) + ": "
                    + Errors.describe(e));
            String hint = Errors.queryHint(side, e);
            if (hint != null) {
                err.println("  check: " + hint);
            }
            return Commands.STOPPED;
        } catch (RuntimeException e) {
            err.println(a.command() + " stopped: " + (c == null ? "setting up the vault clients" : c.step()) + ": "
                    + Errors.describe(e));
            return Commands.STOPPED;
        }
    }

    private static Connection connect(MigrateConfig.Side side) throws SQLException {
        return DriverManager.getConnection(side.jdbcUrl(), side.user(), side.password());
    }

    private static void close(Connection c) {
        if (c != null) {
            try {
                c.close();
            } catch (SQLException ignored) {
                // nothing left to do with it
            }
        }
    }

    static String usage() {
        return "usage: hsm-edek-migrate <analyze|stage|wrap|verify|report|promote|reset> --config <file> "
                + "[--kek <source KEK>[,...]] [--app <app_id>[,...]] [--ids <file>] [--yes] [--no-probe]\n"
                + "       hsm-edek-migrate template > migrate.properties   (every setting, commented)\n"
                + "       hsm-edek-migrate help";
    }

    static final String HELP = """

            commands:
              analyze [--no-probe]  counts A's rows per KEK, version, status and owner; checks the KEK map,
                                    owners and clashes in B; probes both vaults. Writes nothing
              stage                 brings the staging table in line with A. No vault call
              wrap                  unwraps each staged or failed row with A, wraps it with B
              verify                unwraps each wrapped row with both vaults and compares the DEKs
              report                counts per source KEK, owner and status; failed rows; ready or not;
                                    with --ids, also each listed row's status
              promote               copies verified rows into B's edek_records in one transaction
              reset --yes           deletes the staging rows in scope
              template              prints a settings file with every key explained

            limits, for any command (a row must match each one given):
              --kek <source KEK>[,...]  these source KEKs; repeat the flag to add more
              --app <app_id>[,...]      these owners; repeat the flag to add more
              --ids <file>              these edek_ids, one per line (# comments allowed), for a
                                        trial promote of a sample. B can decrypt only promoted
                                        rows, so promote the rest before B takes traffic

            exit: 0 done, 1 stopped (rerun continues), 2 usage or settings error,
                  3 finished with failed rows, problems found, or a refused promote

            Design and runbook: java/docs/EDEK_TENANT_MIGRATION.md""";

    static final String TEMPLATE_RESOURCE = "/migrate.properties.template";

    private static int template(PrintStream out, PrintStream err) {
        try (InputStream in = EdekMigrate.class.getResourceAsStream(TEMPLATE_RESOURCE)) {
            if (in == null) {
                err.println("the settings template is missing from the jar");
                return Commands.STOPPED;
            }
            out.print(new String(in.readAllBytes(), StandardCharsets.UTF_8));
            out.flush();
            return Commands.OK;
        } catch (IOException e) {
            err.println("can't read the settings template: " + Commands.errorType(e));
            return Commands.STOPPED;
        }
    }

    record Args(String command, Path config, Scope scope, boolean yes, boolean probe) {

        static Args parse(String[] args) {
            if (args.length == 0 || !COMMANDS.contains(args[0])) {
                throw new IllegalArgumentException(args.length == 0 ? "no command" : "unknown command: " + args[0]);
            }
            Path config = null;
            Set<String> keks = new TreeSet<>();
            Set<String> apps = new TreeSet<>();
            Set<UUID> ids = new TreeSet<>();
            Path idsFile = null;
            boolean yes = false;
            boolean probe = true;
            List<String> rest = List.of(args).subList(1, args.length);
            for (int i = 0; i < rest.size(); i++) {
                String opt = rest.get(i);
                switch (opt) {
                    case "--config" -> config = Path.of(value(rest, ++i, opt));
                    case "--kek" -> keks.addAll(list(value(rest, ++i, opt), opt));
                    case "--app" -> apps.addAll(list(value(rest, ++i, opt), opt));
                    case "--ids" -> {
                        if (idsFile != null) {
                            throw new IllegalArgumentException("--ids is given twice; put every edek_id in one file");
                        }
                        idsFile = Path.of(value(rest, ++i, opt));
                        ids.addAll(readIds(idsFile));
                    }
                    case "--yes" -> yes = true;
                    case "--no-probe" -> probe = false;
                    default -> throw new IllegalArgumentException("unknown option: " + opt);
                }
            }
            if (config == null) {
                throw new IllegalArgumentException("--config is required");
            }
            return new Args(args[0], config, new Scope(keks, apps, ids), yes, probe);
        }

        /** {@code a,b} or a single name; repeated flags add up. */
        private static List<String> list(String v, String opt) {
            List<String> out = new ArrayList<>();
            for (String part : v.split(",", -1)) {
                String t = part.trim();
                if (t.isEmpty()) {
                    throw new IllegalArgumentException(opt + " has an empty name in '" + v + "'");
                }
                out.add(t);
            }
            return out;
        }

        /** One edek_id per line; blank lines and lines starting with # are skipped. */
        static Set<UUID> readIds(Path file) {
            List<String> lines;
            try {
                lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new IllegalArgumentException("--ids: can't read " + file + ": " + Commands.errorType(e));
            }
            Set<UUID> ids = new TreeSet<>();
            for (int n = 0; n < lines.size(); n++) {
                String t = lines.get(n).trim();
                if (t.isEmpty() || t.startsWith("#")) {
                    continue;
                }
                try {
                    ids.add(UUID.fromString(t));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("--ids: line " + (n + 1) + " of " + file + " isn't an edek_id");
                }
            }
            if (ids.isEmpty()) {
                throw new IllegalArgumentException("--ids: " + file + " has no edek_ids");
            }
            return ids;
        }

        private static String value(List<String> rest, int i, String opt) {
            if (i >= rest.size() || rest.get(i).startsWith("--")) {
                throw new IllegalArgumentException(opt + " needs a value");
            }
            return rest.get(i);
        }
    }
}
