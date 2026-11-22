package com.hsm.client.deidjob;

import com.hsm.client.db.DbDialect;
import com.hsm.client.deid.FieldOptions;
import com.hsm.client.deid.FieldStats;
import com.hsm.client.deid.SurrogateStore.InsertResult;
import com.hsm.client.deid.TokenKeyProvider;
import com.hsm.client.deid.transform.SurrogateTransform;
import com.hsm.client.deidjob.store.JdbcSurrogateStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.hsm.client.deidjob.DeidJobTestSupport.CLOCK_2026;
import static com.hsm.client.deidjob.DeidJobTestSupport.SURROGATE_KEY_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The surrogate table on H2 (PostgreSQL mode) with the shipped Postgres DDL. */
class JdbcSurrogateStoreTest {

    private JdbcTemplate jdbc;
    private JdbcSurrogateStore store;

    @BeforeEach
    void setUp() {
        jdbc = new JdbcTemplate(DeidJobTestSupport.h2());
        DeidJobTestSupport.createSurrogateTable(jdbc);
        store = new JdbcSurrogateStore(jdbc, DbDialect.POSTGRESQL, null, "deid_surrogate", CLOCK_2026);
    }

    private static byte[] hash(int n) {
        byte[] h = new byte[32];
        h[0] = (byte) n;
        h[31] = (byte) (n >> 8);
        return h;
    }

    @Test
    void insertThenFind() {
        store.verifyAvailable();
        assertEquals(InsertResult.INSERTED, store.insertIfAbsent("zen", "member", hash(1), new byte[]{1, 2}, SURROGATE_KEY_ID,
                "S-AAAAA-00001", "run-1"));
        Map<String, String> found = store.find("zen", "member", List.of(hash(1), hash(2)));
        assertEquals(Map.of(HexFormat.of().formatHex(hash(1)), "S-AAAAA-00001"), found);
        assertTrue(store.find("acme", "member", List.of(hash(1))).isEmpty(), "scopes are separate");
        assertEquals(UUID.fromString(jdbc.queryForObject("SELECT CAST(key_edek_id AS varchar) FROM deid_surrogate", String.class)),
                SURROGATE_KEY_ID);
        assertEquals("run-1", jdbc.queryForObject("SELECT created_run_id FROM deid_surrogate", String.class));
    }

    @Test
    void theTwoConflictsAreToldApart() {
        store.insertIfAbsent("zen", "member", hash(1), new byte[]{1}, SURROGATE_KEY_ID, "S-AAAAA-00001", "run-1");
        assertEquals(InsertResult.ROW_EXISTS, store.insertIfAbsent("zen", "member", hash(1), new byte[]{1}, SURROGATE_KEY_ID,
                "S-BBBBB-00002", "run-2"));
        assertEquals(InsertResult.SURROGATE_TAKEN, store.insertIfAbsent("zen", "member", hash(2), new byte[]{1}, SURROGATE_KEY_ID,
                "S-AAAAA-00001", "run-2"));
        assertEquals(InsertResult.INSERTED, store.insertIfAbsent("zen", "card", hash(2), new byte[]{1}, SURROGATE_KEY_ID,
                "S-AAAAA-00001", "run-2"), "the same surrogate in another domain is fine");
    }

    @Test
    void findReadsMoreHashesThanOneInList() {
        List<byte[]> hashes = new ArrayList<>();
        for (int i = 0; i < JdbcSurrogateStore.FIND_CHUNK * 2 + 7; i++) {
            hashes.add(hash(i));
            store.insertIfAbsent("zen", "member", hash(i), new byte[]{1}, SURROGATE_KEY_ID, "S-" + i, "run-1");
        }
        assertEquals(hashes.size(), store.find("zen", "member", hashes).size());
    }

    @Test
    void aMissingTableFailsClearly() {
        JdbcSurrogateStore missing = new JdbcSurrogateStore(jdbc, DbDialect.POSTGRESQL, null, "no_such_table", CLOCK_2026);
        IllegalStateException e = assertThrows(IllegalStateException.class, missing::verifyAvailable);
        assertTrue(e.getMessage().contains("deid/deid-surrogate-postgres.sql"), e.getMessage());
    }

    @Test
    void namesMustBePlainIdentifiers() {
        assertThrows(IllegalArgumentException.class,
                () -> new JdbcSurrogateStore(jdbc, DbDialect.POSTGRESQL, null, "x; DROP TABLE y", CLOCK_2026));
    }

    @Test
    void theTransformOverTheRealTableReusesAndAgreesAcrossConcurrentRuns() throws Exception {
        DeidJobTestSupport.FakeHsm hsm = new DeidJobTestSupport.FakeHsm();
        try (TokenKeyProvider keys = new TokenKeyProvider(hsm::unwrapDek)) {
            String[] members = new String[300];
            for (int i = 0; i < members.length; i++) {
                members[i] = "M" + (2_000_000 + i);
            }
            ExecutorService pool = Executors.newFixedThreadPool(3);
            try {
                List<Callable<List<String>>> runs = new ArrayList<>();
                for (int r = 0; r < 3; r++) {
                    String runId = "run-" + r;
                    runs.add(() -> {
                        SurrogateTransform t = SurrogateTransform.create(new FieldOptions(Map.of()), "member", "zen",
                                keys.keys(SURROGATE_KEY_ID, SurrogateTransform.keyLabels()), SURROGATE_KEY_ID, store, runId);
                        List<Map<String, String>> records = new ArrayList<>();
                        for (String ignored : members) {
                            records.add(Map.of());
                        }
                        return t.applyPage(List.of(members), records, new FieldStats());
                    });
                }
                List<List<String>> results = new ArrayList<>();
                for (Future<List<String>> f : pool.invokeAll(runs)) {
                    results.add(f.get());
                }
                results.forEach(r -> assertEquals(results.get(0), r));
                assertEquals(members.length, jdbc.queryForObject("SELECT COUNT(*) FROM deid_surrogate", Integer.class));
            } finally {
                pool.shutdownNow();
            }

            SurrogateTransform t = SurrogateTransform.create(new FieldOptions(Map.of()), "member", "zen",
                    keys.keys(SURROGATE_KEY_ID, SurrogateTransform.keyLabels()), SURROGATE_KEY_ID, store, "run-9");
            byte[] valueEnc = jdbc.queryForObject("SELECT value_enc FROM deid_surrogate ORDER BY surrogate FETCH FIRST 1 ROWS ONLY",
                    byte[].class);
            assertTrue(t.decryptValue(valueEnc).startsWith("M2"), "maps back to the member id");
        }
    }
}
