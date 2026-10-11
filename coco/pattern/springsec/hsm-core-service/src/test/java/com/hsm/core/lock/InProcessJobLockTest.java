package com.hsm.core.lock;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InProcessJobLockTest {

    private final InProcessJobLock lock = new InProcessJobLock();

    @Test
    void secondCallerIsSkippedWhileFirstHoldsTheKey() throws Exception {
        CountDownLatch acquired = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread holder = new Thread(() -> lock.tryRun("k", () -> {
            acquired.countDown();
            awaitQuietly(release);
            return "holder";
        }));
        holder.start();
        assertTrue(acquired.await(5, TimeUnit.SECONDS));
        try {
            assertEquals(Optional.empty(), lock.tryRun("k", () -> "contender"));
            assertEquals(Optional.of("other-key"), lock.tryRun("other", () -> "other-key"), "different keys must not block each other");
        } finally {
            release.countDown();
            holder.join(5000);
        }
        assertEquals(Optional.of("after"), lock.tryRun("k", () -> "after"));
    }

    @Test
    void lockIsReleasedWhenWorkThrows() {
        assertThrows(IllegalStateException.class, () -> lock.tryRun("k", () -> {
            throw new IllegalStateException("boom");
        }));
        assertEquals(Optional.of(1), lock.tryRun("k", () -> 1));
    }

    static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
