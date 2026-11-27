package com.hsm.edekmigrate;

import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * Error text for the terminal: which database, which step, and the reason the server
 * or SDK gave.
 *
 * <p>What is printed: Postgres's main message and hint (they name objects and users,
 * never row values), the driver's own message for connection failures, and the first
 * line of an Azure error (Entra's AADSTS reason, Key Vault's error code). What is not:
 * Postgres's "Detail" line, which can quote a row, and anything that looks like a
 * password, token or key value. A DEK never reaches an error: the vault SDK doesn't
 * put request bodies in its exceptions.
 */
final class Errors {

    private static final int MAX = 500;
    private static final Pattern SECRETS = Pattern.compile(
            "(?i)\\b(password|passwd|pwd|client_secret|sig|token)=([^&;\\s\"]*)");
    private static final Pattern JSON_VALUE = Pattern.compile("(?i)(\"(?:value|access_token)\"\\s*:\\s*\")[^\"]*\"");
    private static final Pattern BEARER = Pattern.compile("(?i)(bearer\\s+)[A-Za-z0-9._~+/=-]+");

    private Errors() {
    }

    /**
     * {@code "can't connect to the source database host:5432/db as user: reason (SQLState X)"},
     * then a line saying what to check.
     */
    static String connectFailed(String side, MigrateConfig.Side s, SQLException e) {
        return "can't connect to the " + side + " database " + where(s.jdbcUrl()) + " as " + s.user() + ": "
                + describe(e) + "\n  check: " + connectHint(side, e);
    }

    /** {@code "host:port/database"} from a jdbc:postgresql:// URL; otherwise the value as given, so stray quotes show. */
    static String where(String jdbcUrl) {
        String prefix = "jdbc:postgresql://";
        if (jdbcUrl != null && jdbcUrl.startsWith(prefix)) {
            String rest = jdbcUrl.substring(prefix.length());
            int q = rest.indexOf('?');
            return q < 0 ? rest : rest.substring(0, q);
        }
        return "[" + redact(String.valueOf(jdbcUrl)) + "]";
    }

    /** Postgres's message and hint, or the driver's message and its cause; then the SQLState. */
    static String describe(SQLException e) {
        String text;
        ServerErrorMessage m = e instanceof PSQLException p ? p.getServerErrorMessage() : null;
        if (m != null && m.getMessage() != null) {
            text = m.getMessage() + (m.getHint() == null ? "" : "; hint: " + m.getHint());
        } else {
            text = messageAndCause(e);
        }
        return clip(redact(text)) + " (SQLState " + e.getSQLState() + ")";
    }

    /** The type, HTTP status if any, first line of the message, and the root cause's type and message. */
    static String describe(Throwable e) {
        if (e instanceof SQLException s) {
            return describe(s);
        }
        String text = messageAndCause(e);
        String type = Commands.errorType(e);
        return clip(redact(text.isEmpty() ? type : type + ": " + text));
    }

    private static String messageAndCause(Throwable e) {
        String msg = firstLine(e.getMessage());
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        StringBuilder sb = new StringBuilder(msg == null ? "" : msg);
        if (root != e) {
            String rootMsg = firstLine(root.getMessage());
            sb.append(sb.isEmpty() ? "" : " ").append("(cause ").append(root.getClass().getSimpleName());
            if (rootMsg != null && (msg == null || !msg.contains(rootMsg))) {
                sb.append(": ").append(rootMsg);
            }
            sb.append(')');
        }
        return sb.toString();
    }

    /** What to look at for a failed connection, by SQLState. */
    static String connectHint(String side, SQLException e) {
        String state = e.getSQLState() == null ? "" : e.getSQLState();
        String msg = e.getMessage() == null ? "" : e.getMessage();
        if (msg.startsWith("No suitable driver")) {
            return side + ".jdbc-url must look like jdbc:postgresql://<host>:5432/<database>?sslmode=require";
        }
        return switch (state) {
            case "28P01" -> "the server was reached and rejected the user or password: check " + side
                    + ".user, and the password in the variable " + side + ".password-env names (export it in single "
                    + "quotes so the shell doesn't change $, ! or backticks)";
            case "28000" -> "the server was reached and refused this user or machine: check the user exists on this "
                    + "server, that password sign-in is allowed, and sslmode";
            case "3D000" -> "the server was reached, but the database in " + side + ".jdbc-url doesn't exist";
            case "08004" -> "the server was reached and refused the connection: check sslmode in " + side
                    + ".jdbc-url and that the server accepts connections from this machine";
            default -> state.startsWith("08")
                    ? "this machine can't reach the server: check the host and port in " + side + ".jdbc-url, the "
                    + "server's firewall rules, and VPN or private-endpoint DNS (try: nc -vz <host> 5432)"
                    : "see the message above";
        };
    }

    /** What to look at for a failed query, by SQLState; null when there's nothing more to say. */
    static String queryHint(String side, SQLException e) {
        String state = e.getSQLState() == null ? "" : e.getSQLState();
        String schemas = side.equals("target")
                ? "target.crypto-schema, target.access-schema and target.staging-schema; hsm-core's migrations must "
                + "have run on the target"
                : "source.crypto-schema";
        return switch (state) {
            case "42501" -> side + ".user is missing a grant this step needs: see Access in "
                    + "java/hsm-edek-migrate/README.md";
            case "42P01", "3F000" -> "a table or schema isn't there: check " + schemas;
            case "25006" -> "the connection is read-only";
            default -> state.startsWith("08") || state.startsWith("57")
                    ? "the connection dropped or the server stopped; rerun, it continues where this stopped"
                    : null;
        };
    }

    static String redact(String s) {
        if (s == null) {
            return null;
        }
        String r = SECRETS.matcher(s).replaceAll("$1=<redacted>");
        r = JSON_VALUE.matcher(r).replaceAll("$1<redacted>\"");
        return BEARER.matcher(r).replaceAll("$1<redacted>");
    }

    private static String firstLine(String s) {
        if (s == null || s.isBlank()) {
            return null;
        }
        String t = s.strip();
        int nl = t.indexOf('\n');
        int cr = t.indexOf('\r');
        int end = nl < 0 ? cr : cr < 0 ? nl : Math.min(nl, cr);
        return (end < 0 ? t : t.substring(0, end)).strip();
    }

    private static String clip(String s) {
        return s.length() <= MAX ? s : s.substring(0, MAX) + "...";
    }
}
