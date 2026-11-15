package com.hsm.chassis.lock;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link PostgresAdvisoryJobLock} against a real Postgres. Each "pod" gets its
 * own Hikari pool, the way separate replicas/releases do in production, so this
 * proves the lock is shared through the database rather than through the JVM.
 * Skipped when no Docker daemon is available.
 */
@Testcontainers(disabledWithoutDocker = true)
class PostgresAdvisoryJobLockContainerTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    private HikariDataSource podA;
    private HikariDataSource podB;

    @BeforeEach
    void setUp() {
        podA = pool("pod-a");
        podB = pool("pod-b");
    }

    @AfterEach
    void tearDown() {
        podA.close();
        podB.close();
    }

    private static HikariDataSource pool(String name) {
        HikariConfig config = new HikariConfig();
        config.setPoolName(name);
        config.setJdbcUrl(POSTGRES.getJdbcUrl());
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        config.setMaximumPoolSize(3);
        return new HikariDataSource(config);
    }

    @Test
    void onlyOnePodRunsWhileTheOtherIsSkipped() throws Exception {
        JobLock lockA = new PostgresAdvisoryJobLock(podA);
        JobLock lockB = new PostgresAdvisoryJobLock(podB);
        CountDownLatch acquired = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        Thread holder = new Thread(() -> lockA.tryRun("hsm:kek-wrap", () -> {
            acquired.countDown();
            InProcessJobLockTest.awaitQuietly(release);
            return true;
        }));
        holder.start();
        assertTrue(acquired.await(10, TimeUnit.SECONDS));
        try {
            assertEquals(Optional.empty(), lockB.tryRun("hsm:kek-wrap", () -> "b"));
            try (Connection c = podB.getConnection()) {
                String described = PostgresAdvisoryJobLock.describeHolder(c, "hsm:kek-wrap");
                assertTrue(described.startsWith("holder_pid="), described);
                assertTrue(described.contains("holder_user=" + POSTGRES.getUsername()), described);
                assertTrue(described.contains("holder_query=SELECT pg_try_advisory_lock"), described);
                assertEquals("holder=released_before_lookup",
                        PostgresAdvisoryJobLock.describeHolder(c, "hsm:named-dek-rotation"), "a key nobody holds has no holder");
            }
            assertEquals(Optional.of("b"), lockB.tryRun("hsm:named-dek-rotation", () -> "b"),
                    "a different key must not be blocked");
        } finally {
            release.countDown();
            holder.join(10_000);
        }

        assertEquals(Optional.of("b"), lockB.tryRun("hsm:kek-wrap", () -> "b"), "released lock must be acquirable by another pod");
        assertEquals(0, heldAdvisoryLocks(), "no advisory lock may stay held on a pooled connection");
    }

    @Test
    void lockHeldByACrashedPodIsReleasedWhenItsConnectionDrops() throws Exception {
        try (Connection crashed = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             PreparedStatement statement = crashed.prepareStatement("SELECT pg_try_advisory_lock(hashtextextended(?, 0))")) {
            statement.setString(1, "hsm:kek-wrap");
            try (ResultSet rs = statement.executeQuery()) {
                assertTrue(rs.next() && rs.getBoolean(1));
            }
            assertEquals(Optional.empty(), new PostgresAdvisoryJobLock(podB).tryRun("hsm:kek-wrap", () -> "b"));
            int crashedPid;
            try (PreparedStatement pid = crashed.prepareStatement("SELECT pg_backend_pid()");
                 ResultSet rs = pid.executeQuery()) {
                rs.next();
                crashedPid = rs.getInt(1);
            }
            try (Connection c = podB.getConnection()) {
                String described = PostgresAdvisoryJobLock.describeHolder(c, "hsm:kek-wrap");
                assertTrue(described.startsWith("holder_pid=" + crashedPid + " "), "skip must name the exact holding session: " + described);
            }
        }
        // try-with-resources closed the "crashed" pod's connection without an explicit unlock.
        assertEquals(Optional.of("b"), new PostgresAdvisoryJobLock(podB).tryRun("hsm:kek-wrap", () -> "b"));
    }

    private int heldAdvisoryLocks() throws Exception {
        try (Connection c = podA.getConnection();
             PreparedStatement s = c.prepareStatement("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'");
             ResultSet rs = s.executeQuery()) {
            rs.next();
            return rs.getInt(1);
        }
    }
}
