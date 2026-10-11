package com.hsm.core.lock;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * Single-runner guard for work that must never run concurrently across the
 * whole deployment -- every pod of every release (hsm-core-service,
 * hsm-bulk-service, and later v1/v2 during a parallel run) that shares the
 * same database. KEK rotation/rekey/revert and named-DEK rotation are the
 * callers: each pod registers its own scheduler, and the admin API can start
 * the same work by hand, so without this every replica sweeps the same rows
 * at the same cron tick.
 *
 * <p>Non-blocking by design: a caller that can't get the lock is told so
 * immediately (empty result) rather than queuing behind the holder -- a
 * scheduled sweep that lost the race has nothing left to do, and an admin
 * caller is better served by a 409 than a request hanging for the length of
 * a full KEK rotation.
 */
public interface JobLock {

    /**
     * Runs {@code work} only if no other holder currently owns {@code key}.
     * Returns the work's (non-null) result, or empty without running it if the
     * lock is already held elsewhere. The lock is always released when
     * {@code work} returns or throws.
     */
    <T> Optional<T> tryRun(String key, Supplier<T> work);
}
