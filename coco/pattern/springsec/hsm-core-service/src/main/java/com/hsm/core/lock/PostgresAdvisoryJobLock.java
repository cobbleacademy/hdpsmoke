package com.hsm.core.lock;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            if (evict && dataSource.isWrapperFor(HikariDataSource.class)) {
                dataSource.unwrap(HikariDataSource.class).evictConnection(connection);
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
}
