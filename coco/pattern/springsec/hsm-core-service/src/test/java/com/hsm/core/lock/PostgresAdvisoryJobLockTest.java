package com.hsm.core.lock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * JDBC-level contract of {@link PostgresAdvisoryJobLock}, runnable without a
 * database: lock and unlock happen on the same connection, the unlock always
 * runs, and a connection that might still hold the lock never goes back to the
 * pool. Real Postgres behaviour is covered by PostgresAdvisoryJobLockContainerTest.
 */
class PostgresAdvisoryJobLockTest {

    private DataSource dataSource;
    private Connection connection;
    private ResultSet tryLockResult;
    private ResultSet unlockResult;

    @BeforeEach
    void setUp() throws SQLException {
        dataSource = mock(DataSource.class);
        connection = mock(Connection.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(dataSource.isWrapperFor(any())).thenReturn(false);

        tryLockResult = booleanResult(connection, "SELECT pg_try_advisory_lock");
        unlockResult = booleanResult(connection, "SELECT pg_advisory_unlock");
    }

    private static ResultSet booleanResult(Connection connection, String sqlPrefix) throws SQLException {
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(connection.prepareStatement(startsWith(sqlPrefix))).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        return rs;
    }

    @Test
    void runsWorkAndUnlocksOnTheSameConnection() throws SQLException {
        when(tryLockResult.getBoolean(1)).thenReturn(true);
        when(unlockResult.getBoolean(1)).thenReturn(true);

        Optional<String> result = new PostgresAdvisoryJobLock(dataSource).tryRun("k", () -> "done");

        assertEquals(Optional.of("done"), result);
        InOrder order = inOrder(connection);
        order.verify(connection).setAutoCommit(true);
        order.verify(connection).prepareStatement(startsWith("SELECT pg_try_advisory_lock"));
        order.verify(connection).prepareStatement(startsWith("SELECT pg_advisory_unlock"));
        order.verify(connection).close();
        verify(connection, never()).abort(any(Executor.class));
    }

    @Test
    void skipsWithoutRunningOrUnlockingWhenLockIsHeldElsewhere() throws SQLException {
        when(tryLockResult.getBoolean(1)).thenReturn(false);

        Optional<String> result = new PostgresAdvisoryJobLock(dataSource).tryRun("k", () -> {
            throw new AssertionError("work must not run without the lock");
        });

        assertEquals(Optional.empty(), result);
        verify(connection, never()).prepareStatement(startsWith("SELECT pg_advisory_unlock"));
        verify(connection).close();
    }

    @Test
    void unlocksWhenWorkThrows() throws SQLException {
        when(tryLockResult.getBoolean(1)).thenReturn(true);
        when(unlockResult.getBoolean(1)).thenReturn(true);

        assertThrows(IllegalStateException.class, () -> new PostgresAdvisoryJobLock(dataSource).tryRun("k", () -> {
            throw new IllegalStateException("boom");
        }));

        verify(connection).prepareStatement(startsWith("SELECT pg_advisory_unlock"));
        verify(connection).close();
    }

    @Test
    void abortsConnectionInsteadOfPoolingItWhenUnlockFails() throws SQLException {
        when(tryLockResult.getBoolean(1)).thenReturn(true);
        when(unlockResult.getBoolean(1)).thenThrow(new SQLException("connection reset"));

        assertEquals(Optional.of(1), new PostgresAdvisoryJobLock(dataSource).tryRun("k", () -> 1));

        verify(connection).abort(any(Executor.class));
    }
}
