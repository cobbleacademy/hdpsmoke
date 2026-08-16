package com.hsm.core.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * One shared, fixed-size pool bounding concurrent item-level work across
 * EVERY /encrypt/batch and /decrypt/batch request in this process -- not a
 * pool per request -- so hsm.service.batch-executor-pool-size is an
 * aggregate cap on concurrent HSM-bound calls, matching the concern in
 * java/docs/BULK_OPERATIONS.md about self-inflicted HSM throttling from
 * uncontrolled fan-out. Default size 1 makes every batch item run on the
 * same single worker thread, one at a time -- functionally identical to the
 * previous plain sequential for-loop.
 */
@Configuration
public class BatchExecutorConfig {

    @Bean(destroyMethod = "shutdown")
    public ExecutorService batchExecutor(HsmProperties properties) {
        int poolSize = Math.max(1, properties.service().batchExecutorPoolSize());
        AtomicInteger counter = new AtomicInteger();
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable, "batch-executor-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        return Executors.newFixedThreadPool(poolSize, threadFactory);
    }
}
