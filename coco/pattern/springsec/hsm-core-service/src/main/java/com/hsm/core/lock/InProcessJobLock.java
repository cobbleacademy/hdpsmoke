package com.hsm.core.lock;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * {@link JobLock} for non-Postgres databases (the demo profile's H2) -- guards
 * within this JVM only. Demo mode never registers the rotation schedulers, so
 * the only contention it sees is an admin call overlapping another admin call
 * on the same instance, which this covers.
 */
public class InProcessJobLock implements JobLock {

    private final ConcurrentMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    @Override
    public <T> Optional<T> tryRun(String key, Supplier<T> work) {
        ReentrantLock lock = locks.computeIfAbsent(key, k -> new ReentrantLock());
        if (!lock.tryLock()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Objects.requireNonNull(work.get(), "JobLock work must return a non-null result"));
        } finally {
            lock.unlock();
        }
    }
}
