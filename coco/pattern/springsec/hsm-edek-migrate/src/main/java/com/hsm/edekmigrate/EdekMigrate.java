package com.hsm.edekmigrate;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Moves hsm-core's EDEKs from one tenant (A) to another (B) by re-wrapping each DEK:
 * unwrap with A's KEK, wrap with B's, check, then promote into B's edek_records. Design
 * and runbook: java/docs/EDEK_TENANT_MIGRATION.md.
 *
 * <pre>
 * java -jar hsm-edek-migrate.jar &lt;command&gt; --config migrate.properties [--kek NAME] [--app APP_ID] [--yes] [--no-probe]
 * </pre>
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

        try (Connection source = connect(config.source()); Connection target = connect(config.target())) {
            Commands c = new Commands(config, source, target, AzureKekVault.of(config.source()),
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
            err.println(a.command() + " stopped: database error " + Commands.errorType(e) + " SQLState="
                    + e.getSQLState());
            return Commands.STOPPED;
        } catch (RuntimeException e) {
            err.println(a.command() + " stopped: " + Commands.errorType(e));
            return Commands.STOPPED;
        }
    }

    private static Connection connect(MigrateConfig.Side side) throws SQLException {
        return DriverManager.getConnection(side.jdbcUrl(), side.user(), side.password());
    }

    static String usage() {
        return "usage: hsm-edek-migrate <analyze|stage|wrap|verify|report|promote|reset> --config <file> "
                + "[--kek <source KEK>] [--app <app_id>] [--yes] [--no-probe]";
    }

    record Args(String command, Path config, Scope scope, boolean yes, boolean probe) {

        static Args parse(String[] args) {
            if (args.length == 0 || !COMMANDS.contains(args[0])) {
                throw new IllegalArgumentException(args.length == 0 ? "no command" : "unknown command: " + args[0]);
            }
            Path config = null;
            String kek = null;
            String app = null;
            boolean yes = false;
            boolean probe = true;
            List<String> rest = List.of(args).subList(1, args.length);
            for (int i = 0; i < rest.size(); i++) {
                String opt = rest.get(i);
                switch (opt) {
                    case "--config" -> config = Path.of(value(rest, ++i, opt));
                    case "--kek" -> kek = value(rest, ++i, opt);
                    case "--app" -> app = value(rest, ++i, opt);
                    case "--yes" -> yes = true;
                    case "--no-probe" -> probe = false;
                    default -> throw new IllegalArgumentException("unknown option: " + opt);
                }
            }
            if (config == null) {
                throw new IllegalArgumentException("--config is required");
            }
            return new Args(args[0], config, new Scope(kek, app), yes, probe);
        }

        private static String value(List<String> rest, int i, String opt) {
            if (i >= rest.size() || rest.get(i).startsWith("--")) {
                throw new IllegalArgumentException(opt + " needs a value");
            }
            return rest.get(i);
        }
    }
}
