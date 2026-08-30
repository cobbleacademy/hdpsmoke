package com.hsm.client.crypto;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Bounded, TTL-evicting cache keyed by K, holding a value V that carries a
 * plaintext DEK somewhere inside it -- zeroes the DEK bytes it extracts from
 * every evicted V, not just whatever's left at {@link #clear()}. Exists
 * specifically to shrink how long a plaintext DEK sits resident in this
 * process's heap: without this, a cache held a DEK for the entire process
 * lifetime (until an explicit close()), which is exactly the exposure
 * window that matters to a memory-dump attacker on a long-running caller (a
 * Spark executor, a bulk job). Bounding size and age doesn't make dumping
 * memory impossible -- see AUTHORIZATION.md's "mTLS does not address
 * client-side DEK memory exposure" -- it shrinks how much is exposed at any
 * one moment and for how long.
 *
 * <p>V is generic rather than a bare {@code byte[]} because the encrypt-side
 * cache needs to hold an {@code edekId} alongside the DEK bytes, not just
 * the bytes themselves; {@code dekExtractor} tells this class where the
 * zeroable bytes live inside V. For the decrypt-side cache (V = byte[]
 * directly), pass {@code Function.identity()}.
 *
 * <p>Thread-safe. {@link #getOrLoad} is atomic per key -- concurrent callers
 * for the same key never cause {@code loader} to run twice, the same
 * guarantee {@code ConcurrentHashMap.computeIfAbsent} gave the caches this
 * replaces. Eviction (age- or size-triggered) uses
 * {@code ConcurrentHashMap.remove(key, value)}'s compare-and-remove instead
 * of a separate insertion-order structure, so a DEK that was just refreshed
 * by another thread can never be mistakenly zeroed as if it were the stale
 * copy being evicted.
 */
public final class DekCache<K, V> {

    private record Entry<V>(V value, Instant createdAt) {
    }

    private final ConcurrentHashMap<K, Entry<V>> entries = new ConcurrentHashMap<>();
    private final int maxSize;
    private final Duration ttl;
    private final Function<V, byte[]> dekExtractor;

    public DekCache(int maxSize, Duration ttl, Function<V, byte[]> dekExtractor) {
        if (maxSize < 1) {
            throw new IllegalArgumentException("maxSize must be at least 1");
        }
        if (ttl == null || ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("ttl must be a positive duration");
        }
        this.maxSize = maxSize;
        this.ttl = ttl;
        this.dekExtractor = dekExtractor;
    }

    /**
     * Returns the cached value for key, or computes and caches a fresh one
     * via loader if absent or expired. An expired entry's DEK is zeroed
     * before being replaced. May trigger eviction of the single oldest
     * entry if this insert pushes the cache over maxSize.
     */
    public V getOrLoad(K key, Function<K, V> loader) {
        Entry<V> entry = entries.compute(key, (k, existing) -> {
            if (existing != null && !isExpired(existing)) {
                return existing;
            }
            if (existing != null) {
                DekManager.zeroDek(dekExtractor.apply(existing.value()));
            }
            return new Entry<>(loader.apply(k), Instant.now());
        });
        evictOverCapacity();
        return entry.value();
    }

    /** Actively sweeps and zeroes every entry older than ttl, whether or not it's been accessed recently. */
    public void evictExpired() {
        Instant cutoff = Instant.now().minus(ttl);
        for (Map.Entry<K, Entry<V>> e : entries.entrySet()) {
            Entry<V> val = e.getValue();
            if (val.createdAt().isBefore(cutoff) && entries.remove(e.getKey(), val)) {
                DekManager.zeroDek(dekExtractor.apply(val.value()));
            }
        }
    }

    private boolean isExpired(Entry<V> entry) {
        return entry.createdAt().isBefore(Instant.now().minus(ttl));
    }

    private void evictOverCapacity() {
        while (entries.size() > maxSize) {
            Map.Entry<K, Entry<V>> oldest = null;
            for (Map.Entry<K, Entry<V>> e : entries.entrySet()) {
                if (oldest == null || e.getValue().createdAt().isBefore(oldest.getValue().createdAt())) {
                    oldest = e;
                }
            }
            if (oldest == null) {
                return; // raced down to empty already
            }
            if (entries.remove(oldest.getKey(), oldest.getValue())) {
                DekManager.zeroDek(dekExtractor.apply(oldest.getValue().value()));
            }
            // if the conditional remove failed, another thread refreshed this exact
            // key between the scan and the remove -- loop re-evaluates from current state
        }
    }

    public int size() {
        return entries.size();
    }

    /** Zeroes and removes every cached DEK. */
    public void clear() {
        entries.values().forEach(e -> DekManager.zeroDek(dekExtractor.apply(e.value())));
        entries.clear();
    }
}
