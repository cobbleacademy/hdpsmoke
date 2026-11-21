package com.hsm.client.deid;

import com.fasterxml.jackson.databind.JsonNode;
import com.hsm.client.deid.transform.SurrogateTransform;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hsm.client.deid.DeidTestSupport.SURROGATE_KEY_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SurrogateTransformTest {

    private static final String SURROGATE_FORMAT = "S-[0-9A-HJKMNP-TV-Z]{5}-[0-9A-HJKMNP-TV-Z]{5}";

    private final TokenKeyProvider keys = new TokenKeyProvider(DeidTestSupport.standardKeys());
    private final DeidTestSupport.InMemorySurrogateStore store = new DeidTestSupport.InMemorySurrogateStore();

    private SurrogateTransform surrogate(String scope, String domain, SurrogateStore s) {
        return SurrogateTransform.create(new FieldOptions(Map.of()), domain, scope,
                keys.keys(SURROGATE_KEY_ID, SurrogateTransform.keyLabels()), SURROGATE_KEY_ID, s, "run-1");
    }

    private static List<Map<String, String>> records(int n) {
        List<Map<String, String>> r = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            r.add(Map.of());
        }
        return r;
    }

    private static List<String> page(SurrogateTransform t, FieldStats stats, String... values) {
        return t.applyPage(Arrays.asList(values), records(values.length), stats);
    }

    @Test
    void firstSightCreatesARandomSurrogateLaterRunsReuseIt() {
        FieldStats first = new FieldStats();
        List<String> out = page(surrogate("zen", "member", store), first, "M1234567", "M1234567", "M7654321");
        assertTrue(out.get(0).matches(SURROGATE_FORMAT), out.get(0));
        assertEquals(out.get(0), out.get(1), "same member, same surrogate in one page");
        assertNotEquals(out.get(0), out.get(2));
        assertEquals(2, first.counter("new"));

        FieldStats second = new FieldStats();
        List<String> again = page(surrogate("zen", "member", store), second, "M7654321", " M1234567 ");
        assertEquals(List.of(out.get(2), out.get(0)), again, "next run: same surrogates (trimmed)");
        assertEquals(2, second.counter("existing"));
        assertEquals(0, second.counter("new"));
    }

    @Test
    void scopesAndDomainsDontShareSurrogates() {
        String zen = page(surrogate("zen", "member", store), new FieldStats(), "M1234567").get(0);
        String acme = page(surrogate("acme", "member", store), new FieldStats(), "M1234567").get(0);
        String card = page(surrogate("zen", "card", store), new FieldStats(), "M1234567").get(0);
        assertNotEquals(zen, acme);
        assertNotEquals(zen, card);
        assertEquals(3, store.rows.size());
    }

    @Test
    void theRowStoresNoValueInClearAndMapsBack() {
        SurrogateTransform t = surrogate("zen", "member", store);
        String s = page(t, new FieldStats(), "M1234567").get(0);
        DeidTestSupport.InMemorySurrogateStore.Row row = store.rowFor(s);
        assertTrue(!new String(row.valueEnc(), java.nio.charset.StandardCharsets.ISO_8859_1).contains("M1234567"));
        assertEquals(SURROGATE_KEY_ID, row.keyEdekId());
        assertEquals("run-1", row.runId());
        assertEquals("M1234567", t.decryptValue(row.valueEnc()));

        SurrogateTransform otherScope = surrogate("acme", "member", store);
        assertThrows(DeidException.class, () -> otherScope.decryptValue(row.valueEnc()),
                "value_enc is bound to its scope and domain");
    }

    @Test
    void lookupHashesMatchTheTestVectors() {
        JsonNode cases = DeidTestSupport.vectors().get("lookup_hash");
        DeidTestSupport.FakeDekSource source = new DeidTestSupport.FakeDekSource()
                .with(SURROGATE_KEY_ID, DeidTestSupport.VECTOR_DEK);
        try (TokenKeyProvider vectorKeys = new TokenKeyProvider(source)) {
            for (JsonNode c : cases) {
                DeidTestSupport.InMemorySurrogateStore s = new DeidTestSupport.InMemorySurrogateStore();
                SurrogateTransform t = SurrogateTransform.create(new FieldOptions(Map.of()), c.get("domain").asText(),
                        "vectors", vectorKeys.keys(SURROGATE_KEY_ID, SurrogateTransform.keyLabels()), SURROGATE_KEY_ID,
                        s, "run-1");
                page(t, new FieldStats(), c.get("value").asText());
                String hex = s.rows.keySet().iterator().next().split("\\|")[2];
                assertEquals(c.get("hash_hex").asText(), hex);
            }
        }
    }

    @Test
    void aCollidingSurrogateIsRetried() {
        AtomicInteger collisions = new AtomicInteger(2);
        SurrogateStore colliding = new DeidTestSupport.InMemorySurrogateStore() {
            @Override
            public synchronized InsertResult insertIfAbsent(String scope, String domain, byte[] lookupHash, byte[] valueEnc,
                                                            UUID keyEdekId, String surrogate, String runId) {
                if (collisions.getAndDecrement() > 0) {
                    return InsertResult.SURROGATE_TAKEN;
                }
                return super.insertIfAbsent(scope, domain, lookupHash, valueEnc, keyEdekId, surrogate, runId);
            }
        };
        String s = page(surrogate("zen", "member", colliding), new FieldStats(), "M1234567").get(0);
        assertTrue(s.matches(SURROGATE_FORMAT));
    }

    @Test
    void endlessCollisionsFailTheRun() {
        SurrogateStore alwaysTaken = new DeidTestSupport.InMemorySurrogateStore() {
            @Override
            public InsertResult insertIfAbsent(String scope, String domain, byte[] lookupHash, byte[] valueEnc,
                                               UUID keyEdekId, String surrogate, String runId) {
                return InsertResult.SURROGATE_TAKEN;
            }
        };
        DeidException e = assertThrows(DeidException.class,
                () -> page(surrogate("zen", "member", alwaysTaken), new FieldStats(), "M1234567"));
        assertTrue(e.getMessage().contains("5 tries"));
    }

    @Test
    void aLostRaceUsesTheWinnersSurrogate() {
        // Another run inserts the row between our find and our insert.
        DeidTestSupport.InMemorySurrogateStore racing = new DeidTestSupport.InMemorySurrogateStore() {
            boolean raced;

            @Override
            public synchronized InsertResult insertIfAbsent(String scope, String domain, byte[] lookupHash, byte[] valueEnc,
                                                            UUID keyEdekId, String surrogate, String runId) {
                if (!raced) {
                    raced = true;
                    super.insertIfAbsent(scope, domain, lookupHash, valueEnc, keyEdekId, "S-WINNR-00000", "run-other");
                }
                return super.insertIfAbsent(scope, domain, lookupHash, valueEnc, keyEdekId, surrogate, runId);
            }
        };
        FieldStats stats = new FieldStats();
        assertEquals(List.of("S-WINNR-00000"), page(surrogate("zen", "member", racing), stats, "M1234567"));
        assertEquals(1, stats.counter("existing"));
    }

    @Test
    void concurrentRunsAgreeOnOneSurrogatePerMember() throws Exception {
        String[] members = new String[200];
        for (int i = 0; i < members.length; i++) {
            members[i] = "M" + (1_000_000 + i);
        }
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            List<Callable<List<String>>> runs = new ArrayList<>();
            for (int r = 0; r < 4; r++) {
                runs.add(() -> page(surrogate("zen", "member", store), new FieldStats(), members));
            }
            List<List<String>> results = new ArrayList<>();
            for (Future<List<String>> f : pool.invokeAll(runs)) {
                results.add(f.get());
            }
            for (List<String> result : results) {
                assertEquals(results.get(0), result);
            }
            assertEquals(members.length, store.rows.size());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void nullAndEmptyPassThroughWithoutTouchingTheTable() {
        SurrogateStore untouchable = new SurrogateStore() {
            @Override
            public Map<String, String> find(String scope, String domain, List<byte[]> lookupHashes) {
                throw new AssertionError("table read");
            }

            @Override
            public InsertResult insertIfAbsent(String scope, String domain, byte[] lookupHash, byte[] valueEnc,
                                               UUID keyEdekId, String surrogate, String runId) {
                throw new AssertionError("table write");
            }
        };
        List<String> out = page(surrogate("zen", "member", untouchable), new FieldStats(), null, "");
        assertNull(out.get(0));
        assertEquals("", out.get(1));
    }

    @Test
    void lengthAndPrefixOptions() {
        SurrogateTransform t = SurrogateTransform.create(new FieldOptions(Map.of("prefix", "Z", "length", 15)), "member",
                "zen", keys.keys(SURROGATE_KEY_ID, SurrogateTransform.keyLabels()), SURROGATE_KEY_ID, store, "run-1");
        String s = page(t, new FieldStats(), "M1234567").get(0);
        assertTrue(s.matches("Z[0-9A-Z]{5}-[0-9A-Z]{5}-[0-9A-Z]{5}"), s);
        assertThrows(IllegalArgumentException.class, () -> SurrogateTransform.create(new FieldOptions(Map.of()), null,
                "zen", keys.keys(SURROGATE_KEY_ID, SurrogateTransform.keyLabels()), SURROGATE_KEY_ID, store, "run-1"),
                "a surrogate needs a domain");
    }
}
