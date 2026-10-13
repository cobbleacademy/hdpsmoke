package com.hsm.core.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsm.core.config.HsmProperties;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
        return config(port, batchSize, flushIntervalSeconds, 10_000);
    }

    private HsmProperties.Splunk config(int port, int batchSize, int flushIntervalSeconds, int maxQueueSize) {
        return new HsmProperties.Splunk(true, "http://localhost:" + port + "/hec", "test-token",
                "hsm_audit", "hsm-core-service", "_json", false, batchSize, flushIntervalSeconds, maxQueueSize, 10);
    }

    /** Fake HEC that records the event_type of every delivered event, in order; fails while {@code failing} is set. */
    private List<String> recordingHec(AtomicBoolean failing, AtomicInteger requests) throws IOException {
        List<String> delivered = new CopyOnWriteArrayList<>();
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/hec", exchange -> {
            requests.incrementAndGet();
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            if (failing.get()) {
                exchange.sendResponseHeaders(503, -1);
            } else {
                for (String line : body.split("\n")) {
                    delivered.add(MAPPER.readTree(line).at("/event/event_type").asText());
                }
                exchange.sendResponseHeaders(200, -1);
            }
            exchange.close();
        });
        server.start();
        return delivered;
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

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

    @Test
    void oneFlushDrainsTheWholeQueueNotJustOneBatch() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        List<String> delivered = recordingHec(new AtomicBoolean(false), requests);
        SplunkHecBatcher batcher = new SplunkHecBatcher(config(server.getAddress().getPort(), 50, 3600), "test-token");
        for (int i = 0; i < 120; i++) {
            batcher.enqueue(event("e" + i));
        }

        batcher.flush();

        assertEquals(120, delivered.size(), "all queued events delivered in one tick");
        assertEquals(3, requests.get(), "120 events in batches of 50");
        assertEquals("e0", delivered.get(0));
        assertEquals("e119", delivered.get(119));
        assertEquals(0, batcher.queueSize());
    }

    @Test
    void queueIsBoundedDroppingTheOldestEvenWhileSplunkIsDown() throws Exception {
        AtomicBoolean failing = new AtomicBoolean(true);
        AtomicInteger requests = new AtomicInteger();
        List<String> delivered = recordingHec(failing, requests);
        SplunkHecBatcher batcher = new SplunkHecBatcher(config(server.getAddress().getPort(), 5, 3600, 10), "test-token");

        for (int i = 0; i < 25; i++) {
            batcher.enqueue(event("e" + i));
            batcher.flush(); // Splunk down: every attempt fails and is requeued
        }

        assertEquals(10, batcher.queueSize(), "never more than max-queue-size in memory");
        assertEquals(15, batcher.droppedTotal());

        failing.set(false);
        batcher.flush();
        assertEquals(List.of("e15", "e16", "e17", "e18", "e19", "e20", "e21", "e22", "e23", "e24"), delivered,
                "the newest events survive, in order");
    }

    @Test
    void shutdownSendsEverythingStillQueued() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        List<String> delivered = recordingHec(new AtomicBoolean(false), requests);
        SplunkHecBatcher batcher = new SplunkHecBatcher(config(server.getAddress().getPort(), 50, 3600), "test-token");
        for (int i = 0; i < 130; i++) {
            batcher.enqueue(event("e" + i));
        }

        batcher.stop();

        assertEquals(130, delivered.size(), "not just one final batch of 50");
    }
}
