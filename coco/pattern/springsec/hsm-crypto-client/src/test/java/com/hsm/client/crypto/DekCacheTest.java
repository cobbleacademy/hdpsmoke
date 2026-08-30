package com.hsm.client.crypto;

import com.hsm.client.config.FipsBootstrap;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DekCacheTest {

    static {
        // DekManager.zeroDek is plain Arrays.fill and needs no provider, but
        // DekManager's own static initializer (CryptoServicesRegistrar.getSecureRandom())
        // does -- register BC-FIPS the same way HsmCryptoClient does for any real caller.
        FipsBootstrap.register();
    }

    private static byte[] dekOf(int b) {
        byte[] dek = new byte[32];
        dek[0] = (byte) b;
        return dek;
    }

    @Test
    void getOrLoad_reusesCachedValueWithoutReinvokingLoader() {
        DekCache<String, byte[]> cache = new DekCache<>(10, Duration.ofMinutes(30), Function.identity());
        AtomicInteger loadCount = new AtomicInteger();

        byte[] first = cache.getOrLoad("name-a", k -> {
            loadCount.incrementAndGet();
            return dekOf(1);
        });
        byte[] second = cache.getOrLoad("name-a", k -> {
            loadCount.incrementAndGet();
            return dekOf(2);
        });

        assertEquals(1, loadCount.get());
        assertSame(first, second);
        assertEquals(1, cache.size());
    }

    @Test
    void getOrLoad_expiredEntryIsReplacedAndOldDekIsZeroed() throws InterruptedException {
        DekCache<String, byte[]> cache = new DekCache<>(10, Duration.ofMillis(20), Function.identity());
        byte[] original = cache.getOrLoad("name-a", k -> dekOf(1));

        Thread.sleep(60); // let it pass the 20ms ttl

        byte[] refreshed = cache.getOrLoad("name-a", k -> dekOf(2));

        assertNotSame(original, refreshed);
        assertArrayEquals(new byte[32], original); // zeroed on eviction, not just discarded
        assertEquals((byte) 2, refreshed[0]);
    }

    @Test
    void evictExpired_proactivelyZeroesEntriesNeverAccessedAgain() throws InterruptedException {
        DekCache<String, byte[]> cache = new DekCache<>(10, Duration.ofMillis(20), Function.identity());
        byte[] dek = cache.getOrLoad("name-a", k -> dekOf(9));

        Thread.sleep(60);
        cache.evictExpired();

        assertEquals(0, cache.size());
        assertArrayEquals(new byte[32], dek); // proactively zeroed by the sweep, no read ever triggered it
    }

    @Test
    void evictExpired_leavesFreshEntriesUntouched() {
        DekCache<String, byte[]> cache = new DekCache<>(10, Duration.ofMinutes(30), Function.identity());
        byte[] dek = cache.getOrLoad("name-a", k -> dekOf(7));

        cache.evictExpired();

        assertEquals(1, cache.size());
        assertEquals((byte) 7, dek[0]);
    }

    @Test
    void getOrLoad_evictsSingleOldestEntryWhenOverCapacity() {
        DekCache<String, byte[]> cache = new DekCache<>(2, Duration.ofMinutes(30), Function.identity());
        cache.getOrLoad("a", k -> dekOf(1));
        cache.getOrLoad("b", k -> dekOf(2));
        assertEquals(2, cache.size());

        cache.getOrLoad("c", k -> dekOf(3));

        // still bounded at maxSize -- exactly one eviction per insert-over-capacity
        assertEquals(2, cache.size());
    }

    @Test
    void getOrLoad_neverExceedsMaxSizeAcrossManyInserts() {
        int maxSize = 5;
        DekCache<Integer, byte[]> cache = new DekCache<>(maxSize, Duration.ofMinutes(30), Function.identity());
        for (int i = 0; i < 50; i++) {
            int key = i;
            cache.getOrLoad(key, k -> dekOf(1));
            assertTrue(cache.size() <= maxSize, "cache grew past maxSize at insert " + i);
        }
        assertEquals(maxSize, cache.size());
    }

    @Test
    void clear_zeroesAndRemovesEveryEntry() {
        DekCache<String, byte[]> cache = new DekCache<>(10, Duration.ofMinutes(30), Function.identity());
        byte[] a = cache.getOrLoad("a", k -> dekOf(1));
        byte[] b = cache.getOrLoad("b", k -> dekOf(2));

        cache.clear();

        assertEquals(0, cache.size());
        assertArrayEquals(new byte[32], a);
        assertArrayEquals(new byte[32], b);
    }

    @Test
    void getOrLoad_worksWithACompositeValueTypeViaExtractor() {
        record CachedDek(java.util.UUID edekId, byte[] dek) {
        }
        DekCache<String, CachedDek> cache = new DekCache<>(10, Duration.ofMinutes(30), CachedDek::dek);
        java.util.UUID id = java.util.UUID.randomUUID();

        CachedDek loaded = cache.getOrLoad("name-a", k -> new CachedDek(id, dekOf(5)));

        assertEquals(id, loaded.edekId());
        cache.clear();
        assertArrayEquals(new byte[32], loaded.dek()); // extractor found the right bytes to zero
    }

    @Test
    void getOrLoad_failedLoaderLeavesCacheUsableForSubsequentCalls() {
        DekCache<String, byte[]> cache = new DekCache<>(10, Duration.ofMinutes(30), Function.identity());

        assertThrows(RuntimeException.class, () -> cache.getOrLoad("name-a", k -> {
            throw new RuntimeException("simulated /dek/issue failure");
        }));
        assertEquals(0, cache.size());

        byte[] dek = cache.getOrLoad("name-a", k -> dekOf(3));
        assertEquals((byte) 3, dek[0]);
        assertEquals(1, cache.size());
    }

    @Test
    void getOrLoad_concurrentCallsForSameKeyInvokeLoaderExactlyOnce() throws InterruptedException {
        DekCache<String, byte[]> cache = new DekCache<>(10, Duration.ofMinutes(30), Function.identity());
        AtomicInteger loadCount = new AtomicInteger();
        int threads = 20;
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<java.util.concurrent.Future<byte[]>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    return cache.getOrLoad("shared-name", k -> {
                        loadCount.incrementAndGet();
                        try {
                            Thread.sleep(20); // widen the race window
                        } catch (InterruptedException ignored) {
                        }
                        return dekOf(42);
                    });
                }));
            }
            ready.await();
            go.countDown();
            for (var f : futures) {
                byte[] result = f.get();
                assertEquals((byte) 42, result[0]);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }
        assertEquals(1, loadCount.get(), "loader must run exactly once under concurrent access to the same key");
    }

    @Test
    void constructor_rejectsInvalidMaxSizeAndTtl() {
        assertThrows(IllegalArgumentException.class, () -> new DekCache<String, byte[]>(0, Duration.ofMinutes(1), Function.identity()));
        assertThrows(IllegalArgumentException.class, () -> new DekCache<String, byte[]>(10, Duration.ZERO, Function.identity()));
        assertThrows(IllegalArgumentException.class, () -> new DekCache<String, byte[]>(10, Duration.ofMinutes(-1), Function.identity()));
    }
}
