package com.hsm.client.deidjob;

import com.hsm.client.config.ClientProperties;
import com.hsm.client.db.DbDialect;
import com.hsm.client.deid.SurrogateStore.InsertResult;
import com.hsm.client.deidjob.store.JdbcSurrogateStore;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.EnabledIfDockerAvailable;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static com.hsm.client.deidjob.DeidJobTestSupport.CLOCK_2026;
import static com.hsm.client.deidjob.DeidJobTestSupport.SURROGATE_KEY_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The shipped surrogate-table DDL and {@link JdbcSurrogateStore} on the three real
 * databases, and the whole DEID job on Postgres.
 *
 * <p>Skipped, not failed, without a Docker daemon (as {@code CheckpointStoreTest}).
 * Run with {@code mvn -pl hsm-bulk-client test -Dtest=DeidContainerTest} where Docker
 * is available; the Oracle image is slow to start.
 */
@Testcontainers
@EnabledIfDockerAvailable
class DeidContainerTest {

    private static byte[] hash(int n) {
        byte[] h = new byte[32];
        h[0] = (byte) n;
        return h;
    }

    /** Insert, find, and the two conflicts, on a freshly created table. */
    static void storeBehaves(JdbcTemplate jdbc, DbDialect dialect) {
        DeidJobTestSupport.runDdl(jdbc, JdbcSurrogateStore.ddlResource(dialect));
        JdbcSurrogateStore store = new JdbcSurrogateStore(jdbc, dialect, null, "deid_surrogate", CLOCK_2026);
        store.verifyAvailable();
        assertEquals(InsertResult.INSERTED, store.insertIfAbsent("zen", "member", hash(1), new byte[]{1, 2, 3},
                SURROGATE_KEY_ID, "S-AAAAA-00001", "run-1"));
        assertEquals(InsertResult.ROW_EXISTS, store.insertIfAbsent("zen", "member", hash(1), new byte[]{1},
                SURROGATE_KEY_ID, "S-BBBBB-00002", "run-2"));
        assertEquals(InsertResult.SURROGATE_TAKEN, store.insertIfAbsent("zen", "member", hash(2), new byte[]{1},
                SURROGATE_KEY_ID, "S-AAAAA-00001", "run-2"));
        assertEquals(Map.of(HexFormat.of().formatHex(hash(1)), "S-AAAAA-00001"),
                store.find("zen", "member", List.of(hash(1), hash(2))));
    }

    @Nested
    class Postgres {
        @Container
        static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16-alpine");

        @TempDir
        Path dir;

        private DriverManagerDataSource ds() {
            return new DriverManagerDataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        }

        @Test
        void surrogateStore() {
            JdbcTemplate jdbc = new JdbcTemplate(ds());
            jdbc.execute("DROP TABLE IF EXISTS deid_surrogate");
            storeBehaves(jdbc, DbDialect.POSTGRESQL);
        }

        @Test
        void tableToSafeHarborCsv() throws Exception {
            DriverManagerDataSource ds = ds();
            JdbcTemplate jdbc = new JdbcTemplate(ds);
            jdbc.execute("DROP TABLE IF EXISTS deid_surrogate");
            jdbc.execute("DROP TABLE IF EXISTS giftcards");
            DeidJobTestSupport.FakeHsm hsm = new DeidJobTestSupport.FakeHsm();
            DeidJobTestSupport.createGiftcards(jdbc, hsm, true);
            DeidJobTestSupport.createSurrogateTable(jdbc);
            Path profile = dir.resolve("zen.yml");
            Files.writeString(profile, """
                    id: zen-reporting
                    version: 1
                    vendor: zen-analytics
                    hipaa-basis: safe-harbor
                    surrogate-scope: zen-analytics
                    fields:
                      - { name: member_id,   phi: member-id,   method: surrogate, domain: member }
                      - { name: card_number, phi: card-number, decrypt: true, method: surrogate, domain: card }
                      - { name: dob,         phi: birth-date,  method: generalize, to: birth-year }
                      - { name: issued_date, phi: date,        method: generalize, to: year }
                      - { name: zip,         phi: zip,         method: generalize, to: zip3 }
                      - { name: amount,                        method: passthrough }
                    output: { file: "zen_{yyyyMMdd}.csv" }
                    """);
            ClientProperties.Deid deid = new ClientProperties.Deid(profile.toString(), null,
                    new ClientProperties.Deid.Output(dir.resolve("out").toString()),
                    new ClientProperties.Deid.Surrogate(SURROGATE_KEY_ID.toString(), null, null), false, 0, null);
            ClientProperties.Db db = new ClientProperties.Db(new ClientProperties.Db.TableRef(postgres.getJdbcUrl(),
                    postgres.getUsername(), postgres.getPassword(), null, "giftcards", null, null),
                    null, "id", null, false, List.of(), List.of(), 2, null, 1);
            DeidJob.Result r = new DeidJob(deid, db, () -> hsm, () -> ds, CLOCK_2026).run();

            List<String> lines = Files.readAllLines(r.csv());
            assertEquals("member_id,card_number,dob,issued_date,zip,amount", lines.get(0));
            assertTrue(lines.get(1).endsWith(",1985,2026,606,25.00"), lines.get(1));
            assertTrue(lines.get(2).endsWith(",<=1936,2026,000,100.00"), lines.get(2));
            assertEquals(5, jdbc.queryForObject("SELECT COUNT(*) FROM deid_surrogate", Integer.class));
        }
    }

    @Nested
    class SqlServer {
        @Container
        static final MSSQLServerContainer sqlServer =
                new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2022-latest").acceptLicense();

        @Test
        void surrogateStore() {
            JdbcTemplate jdbc = new JdbcTemplate(new DriverManagerDataSource(sqlServer.getJdbcUrl(),
                    sqlServer.getUsername(), sqlServer.getPassword()));
            jdbc.execute("IF OBJECT_ID('deid_surrogate') IS NOT NULL DROP TABLE deid_surrogate");
            storeBehaves(jdbc, DbDialect.SQL_SERVER);
        }
    }

    @Nested
    class Oracle {
        @Container
        static final OracleContainer oracle = new OracleContainer("gvenzl/oracle-free:23.9-slim-faststart");

        @Test
        void surrogateStore() {
            JdbcTemplate jdbc = new JdbcTemplate(new DriverManagerDataSource(oracle.getJdbcUrl(),
                    oracle.getUsername(), oracle.getPassword()));
            jdbc.execute("BEGIN EXECUTE IMMEDIATE 'DROP TABLE deid_surrogate'; EXCEPTION WHEN OTHERS THEN NULL; END;");
            storeBehaves(jdbc, DbDialect.ORACLE);
        }
    }
}
