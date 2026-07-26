package com.hsm.encryption.audit;

import com.hsm.encryption.config.HsmProperties;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies SplunkHecBatcher's batching and failure-requeue behavior against a
 * lightweight local HTTP server, mirroring tests/unit/test_audit_logger.py's intent.
 */
class SplunkHecBatcherTest {

    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private HsmProperties.Splunk config(int port, int batchSize, int flushIntervalSeconds) {
        return new HsmProperties.Splunk(true, "http://localhost:" + port + "/hec", "test-token",
                "hsm_audit", "hsm-core-service", "_json", false, batchSize, flushIntervalSeconds);
    }

    private Map<String, Object> event(String type) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("event_type", type);
        e.put("_epoch", 1234.5);
        e.put("status", "success");
        return e;
    }

    @Test
    void deliversQueuedEventsOnFlush() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        AtomicInteger requestCount = new AtomicInteger();
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/hec", exchange -> {
            requestCount.incrementAndGet();
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
            received.countDown();
        });
        server.start();

        SplunkHecBatcher batcher = new SplunkHecBatcher(config(server.getAddress().getPort(), 50, 1), "test-token");
        batcher.enqueue(event("encrypt"));
        batcher.start();

        assertTrue(received.await(5, TimeUnit.SECONDS), "expected the batch to be delivered");
        batcher.stop();
        assertEquals(1, requestCount.get());
    }

    @Test
    void requeuesBatchAtFrontOnDeliveryFailure() throws Exception {
        AtomicInteger requestCount = new AtomicInteger();
        CountDownLatch secondAttempt = new CountDownLatch(1);
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/hec", exchange -> {
            int n = requestCount.incrementAndGet();
            if (n == 1) {
                exchange.sendResponseHeaders(500, -1); // first attempt fails
            } else {
                exchange.sendResponseHeaders(200, -1); // retry succeeds
                secondAttempt.countDown();
            }
            exchange.close();
        });
        server.start();

        SplunkHecBatcher batcher = new SplunkHecBatcher(config(server.getAddress().getPort(), 50, 1), "test-token");
        batcher.enqueue(event("decrypt"));
        batcher.start();

        assertTrue(secondAttempt.await(5, TimeUnit.SECONDS), "expected a retry after the first failed delivery");
        batcher.stop();
        assertTrue(requestCount.get() >= 2, "expected at least 2 delivery attempts (fail then retry), got " + requestCount.get());
    }
}
