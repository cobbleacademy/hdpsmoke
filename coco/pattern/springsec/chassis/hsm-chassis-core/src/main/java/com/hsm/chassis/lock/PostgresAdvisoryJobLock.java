package com.hsm.chassis.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@link JobLock} backed by a Postgres session-level advisory lock -- shared by
 * every pod and release pointed at the same database, with no lock table, no
 * migration and no extra dependency.
 *
 * <p>Session-level (not {@code pg_try_advisory_xact_lock}) because the guarded
 * work runs many short transactions of its own (RotationService commits per
 * 200-row page), so no single transaction spans the whole job. Session locks
 * belong to the connection that took them, which is why this borrows one
 * dedicated connection for the job's whole lifetime and both locks and unlocks
 * on it -- the work's own transactions use other pool connections as normal.
 * That also means this needs a direct Postgres connection: a transaction-mode
 * PgBouncer in between would hand the unlock to a different server session.
 *
 * <p>If the pod dies mid-job, the connection drops and Postgres releases the
 * lock on its own -- no expiry to guess, unlike a lock-table approach. A pod
 * that hangs without its TCP connection closing is only noticed via TCP
 * keepalive, hence the tcpKeepAlive=true recommendation on DATABASE_URL.
 *
 * <p>Lock ids come from {@code hashtextextended(key, 0)} (Postgres 11+), so
 * every release derives the same 64-bit id from the same key string.
 */
public class PostgresAdvisoryJobLock implements JobLock {

    private static final Logger log = LoggerFactory.getLogger(PostgresAdvisoryJobLock.class);

    private static final String TRY_LOCK_SQL = "SELECT pg_try_advisory_lock(hashtextextended(?, 0))";
    private static final String UNLOCK_SQL = "SELECT pg_advisory_unlock(hashtextextended(?, 0))";

    /**
     * The session holding {@code key} in this database. A bigint advisory key shows in
     * pg_locks as its high half in classid and its low half in objid, with objsubid = 1.
     * Without pg_read_all_stats, Postgres hides client_addr/state/query of sessions
     * belonging to other roles -- pid, user and application_name still show.
     */
    static final String HOLDER_SQL = """
            SELECT l.pid, a.usename, a.application_name, host(a.client_addr) AS client_addr,
                   a.backend_start, a.state, a.state_change, left(a.query, 120) AS query
            FROM pg_locks l LEFT JOIN pg_stat_activity a ON a.pid = l.pid
            WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 1
              AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database())
              AND l.classid = ((hashtextextended(?, 0) >> 32) & 4294967295)::oid
              AND l.objid = (hashtextextended(?, 0) & 4294967295)::oid
            LIMIT 1""";

    /** Hikari is optional on the chassis classpath; only referenced when present. */
    private static final boolean HIKARI_PRESENT =
            ClassUtils.isPresent("com.zaxxer.hikari.HikariDataSource", PostgresAdvisoryJobLock.class.getClassLoader());

    private final DataSource dataSource;

    public PostgresAdvisoryJobLock(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public <T> Optional<T> tryRun(String key, Supplier<T> work) {
        Connection connection;
        try {
            connection = dataSource.getConnection();
            // Autocommit, so the lock query doesn't leave this session idle-in-transaction
            // for the whole (possibly hours-long) job.
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            throw new IllegalStateException("job_lock_connection_failed key=" + key, e);
        }

        boolean locked = false;
        boolean released = false;
        try {
            locked = queryBoolean(connection, TRY_LOCK_SQL, key);
            if (!locked) {
                log.info("job_lock_held_elsewhere key={} {}", key, describeHolder(connection, key));
                return Optional.empty();
            }
            return Optional.of(Objects.requireNonNull(work.get(), "JobLock work must return a non-null result"));
        } catch (SQLException e) {
            throw new IllegalStateException("job_lock_acquire_failed key=" + key, e);
        } finally {
            if (locked) {
                released = unlock(connection, key);
            }
            // A session lock left on a pooled connection would outlive this job and block
            // every later run on every pod, so a connection whose unlock didn't succeed is
            // evicted (physically closed, which releases the lock server-side) instead of
            // returned to the pool.
            release(connection, locked && !released);
        }
    }

    /**
     * Who holds {@code key}, for the skip log line -- so a skip names the pod (client
     * address) or session that won instead of leaving it to be hunted for. Diagnostic
     * only: any failure is reported in the text, never thrown, so it can't turn a
     * normal skip into an error.
     */
    static String describeHolder(Connection connection, String key) {
        try (PreparedStatement statement = connection.prepareStatement(HOLDER_SQL)) {
            statement.setString(1, key);
            statement.setString(2, key);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    // Released between our try-lock and this query (a short run elsewhere).
                    return "holder=released_before_lookup";
                }
                return "holder_pid=" + rs.getInt("pid")
                        + " holder_client_addr=" + rs.getString("client_addr")
                        + " holder_user=" + rs.getString("usename")
                        + " holder_application=" + rs.getString("application_name")
                        + " holder_backend_start=" + rs.getString("backend_start")
                        + " holder_state=" + rs.getString("state")
                        + " holder_state_change=" + rs.getString("state_change")
                        + " holder_query=" + rs.getString("query");
            }
        } catch (Exception e) {
            return "holder=unknown lookup_error=" + e.getMessage();
        }
    }

    private boolean unlock(Connection connection, String key) {
        try {
            boolean unlocked = queryBoolean(connection, UNLOCK_SQL, key);
            if (!unlocked) {
                log.warn("job_lock_unlock_not_held key={}", key);
            }
            return unlocked;
        } catch (SQLException e) {
            log.warn("job_lock_unlock_failed key={} error={}", key, e.getMessage());
            return false;
        }
    }

    private void release(Connection connection, boolean evict) {
        try {
            if (evict && HIKARI_PRESENT && Hikari.evict(dataSource, connection)) {
                return;
            }
            if (evict) {
                connection.abort(Runnable::run);
            }
            connection.close();
        } catch (SQLException e) {
            log.warn("job_lock_connection_release_failed error={}", e.getMessage());
        }
    }

    private static boolean queryBoolean(Connection connection, String sql, String key) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, key);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    /** Isolated so HikariDataSource is only linked when Hikari is actually on the classpath. */
    private static final class Hikari {

        static boolean evict(DataSource dataSource, Connection connection) throws SQLException {
            if (!dataSource.isWrapperFor(com.zaxxer.hikari.HikariDataSource.class)) {
                return false;
            }
            dataSource.unwrap(com.zaxxer.hikari.HikariDataSource.class).evictConnection(connection);
            return true;
        }
    }
}
