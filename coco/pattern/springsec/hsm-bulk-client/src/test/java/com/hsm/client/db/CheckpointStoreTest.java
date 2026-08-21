package com.hsm.client.db;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.EnabledIfDockerAvailable;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real-container proof that CheckpointStore's dialect-specific SQL (existence check,
 * DDL, MERGE/ON CONFLICT upsert) is actually correct against each of the three real
 * engines DbBulkJob supports -- not just syntactically plausible from reading Oracle/
 * Microsoft's own documentation, which is as far as this could be checked without a
 * live database (see the specific SQL Server MERGE-semicolon and Oracle MERGE-USING
 * patterns confirmed against Microsoft Learn/Oracle's own official docs while writing
 * CheckpointStore).
 *
 * <p><b>Docker availability caveat:</b> {@code @EnabledIfDockerAvailable} means this
 * class is SKIPPED, not failed, wherever no Docker daemon is reachable -- confirmed
 * live in the environment this was authored in (no Docker daemon there; without this
 * annotation, {@code mvn test} hard-failed with "Could not find a valid Docker
 * environment" instead of skipping, which would have broken this module's default
 * build in any environment without Docker, not just this one). Written and (as far as
 * static compilation and API usage can prove) correct, but the actual container runs
 * themselves are NOT verified in the environment this was authored in. Run with
 * {@code mvn -pl hsm-bulk-client test -Dtest=CheckpointStoreTest} wherever Docker is
 * available; first run will be slow (image pulls), the Oracle nested class especially
 * so (gvenzl/oracle-free takes noticeably longer to become ready than Postgres or SQL
 * Server, "faststart" tag notwithstanding).
 *
 * <p>Each nested class covers one dialect end to end: ensureTable creates the table,
 * ensureTable again is a no-op (idempotent, doesn't error on an already-existing
 * table), save() inserts a new row then upserts (updates) an existing one, and
 * loadLastKey/clear round-trip correctly. A fresh, randomly-suffixed table name per
 * test method avoids any cross-test interference within a class's shared (static,
 * started-once) container.
 */
@Testcontainers
@EnabledIfDockerAvailable
class CheckpointStoreTest {

    private static String uniqueTable(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    @Nested
    class Postgres {
        @Container
        static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16-alpine");

        private JdbcTemplate jdbc() {
            DriverManagerDataSource ds = new DriverManagerDataSource(
                    postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
            return new JdbcTemplate(ds);
        }

        @Test
        void ensureTable_createsThenIsIdempotent() {
            JdbcTemplate jdbc = jdbc();
            CheckpointStore store = new CheckpointStore(uniqueTable("chk"), DbDialect.POSTGRESQL);
            store.ensureTable(jdbc);
            store.ensureTable(jdbc); // must not throw on an already-existing table
        }

        @Test
        void save_insertsThenUpserts_andLoadLastKeyRoundTrips() {
            JdbcTemplate jdbc = jdbc();
            CheckpointStore store = new CheckpointStore(uniqueTable("chk"), DbDialect.POSTGRESQL);
            store.ensureTable(jdbc);

            store.save(jdbc, "job-1", 100L, 10);
            assertThat(store.loadLastKey(jdbc, "job-1")).contains("100");

            store.save(jdbc, "job-1", 200L, 20); // same job_id -- must UPDATE, not fail on a duplicate key
            assertThat(store.loadLastKey(jdbc, "job-1")).contains("200");

            assertThat(store.loadLastKey(jdbc, "no-such-job")).isEmpty();
        }

        @Test
        void clear_removesPriorProgress() {
            JdbcTemplate jdbc = jdbc();
            CheckpointStore store = new CheckpointStore(uniqueTable("chk"), DbDialect.POSTGRESQL);
            store.ensureTable(jdbc);
            store.save(jdbc, "job-1", 100L, 10);

            store.clear(jdbc, "job-1");

            assertThat(store.loadLastKey(jdbc, "job-1")).isEmpty();
        }
    }

    @Nested
    class SqlServer {
        @Container
        static final MSSQLServerContainer sqlServer =
                new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2022-latest").acceptLicense();

        private JdbcTemplate jdbc() {
            DriverManagerDataSource ds = new DriverManagerDataSource(
                    sqlServer.getJdbcUrl(), sqlServer.getUsername(), sqlServer.getPassword());
            return new JdbcTemplate(ds);
        }

        @Test
        void ensureTable_createsThenIsIdempotent() {
            JdbcTemplate jdbc = jdbc();
            CheckpointStore store = new CheckpointStore(uniqueTable("chk"), DbDialect.SQL_SERVER);
            store.ensureTable(jdbc);
            store.ensureTable(jdbc); // OBJECT_ID-based exists check must correctly see the table the second time
        }

        @Test
        void save_insertsThenUpserts_andLoadLastKeyRoundTrips() {
            JdbcTemplate jdbc = jdbc();
            CheckpointStore store = new CheckpointStore(uniqueTable("chk"), DbDialect.SQL_SERVER);
            store.ensureTable(jdbc);

            store.save(jdbc, "job-1", 100L, 10);
            assertThat(store.loadLastKey(jdbc, "job-1")).contains("100");

            // Exercises the MERGE statement's WHEN MATCHED branch specifically --
            // the riskiest part of the SQL Server SQL to get right (terminating
            // semicolon, USING-subquery column aliasing).
            store.save(jdbc, "job-1", 200L, 20);
            assertThat(store.loadLastKey(jdbc, "job-1")).contains("200");

            assertThat(store.loadLastKey(jdbc, "no-such-job")).isEmpty();
        }

        @Test
        void clear_removesPriorProgress() {
            JdbcTemplate jdbc = jdbc();
            CheckpointStore store = new CheckpointStore(uniqueTable("chk"), DbDialect.SQL_SERVER);
            store.ensureTable(jdbc);
            store.save(jdbc, "job-1", 100L, 10);

            store.clear(jdbc, "job-1");

            assertThat(store.loadLastKey(jdbc, "job-1")).isEmpty();
        }
    }

    @Nested
    class Oracle {
        @Container
        static final OracleContainer oracle = new OracleContainer("gvenzl/oracle-free:23.9-slim-faststart");

        private JdbcTemplate jdbc() {
            DriverManagerDataSource ds = new DriverManagerDataSource(
                    oracle.getJdbcUrl(), oracle.getUsername(), oracle.getPassword());
            return new JdbcTemplate(ds);
        }

        @Test
        void ensureTable_createsThenIsIdempotent() {
            JdbcTemplate jdbc = jdbc();
            CheckpointStore store = new CheckpointStore(uniqueTable("chk"), DbDialect.ORACLE);
            // The first call's exists-check deliberately triggers and catches a real
            // ORA-00942 (table or view does not exist) internally -- see
            // CheckpointStore.tableExists's ORACLE branch.
            store.ensureTable(jdbc);
            store.ensureTable(jdbc); // second call's exists-check must now see the table via the real SELECT-based probe, not throw
        }

        @Test
        void save_insertsThenUpserts_andLoadLastKeyRoundTrips() {
            JdbcTemplate jdbc = jdbc();
            CheckpointStore store = new CheckpointStore(uniqueTable("chk"), DbDialect.ORACLE);
            store.ensureTable(jdbc);

            store.save(jdbc, "job-1", 100L, 10);
            assertThat(store.loadLastKey(jdbc, "job-1")).contains("100");

            // Exercises the MERGE statement's WHEN MATCHED branch -- the riskiest part
            // of the Oracle SQL to get right (USING (SELECT ... FROM dual) with all
            // three values aliased as named columns, referenced via src.* in both
            // branches rather than re-bound per branch).
            store.save(jdbc, "job-1", 200L, 20);
            assertThat(store.loadLastKey(jdbc, "job-1")).contains("200");

            assertThat(store.loadLastKey(jdbc, "no-such-job")).isEmpty();
        }

        @Test
        void clear_removesPriorProgress() {
            JdbcTemplate jdbc = jdbc();
            CheckpointStore store = new CheckpointStore(uniqueTable("chk"), DbDialect.ORACLE);
            store.ensureTable(jdbc);
            store.save(jdbc, "job-1", 100L, 10);

            store.clear(jdbc, "job-1");

            assertThat(store.loadLastKey(jdbc, "job-1")).isEmpty();
        }
    }
}
