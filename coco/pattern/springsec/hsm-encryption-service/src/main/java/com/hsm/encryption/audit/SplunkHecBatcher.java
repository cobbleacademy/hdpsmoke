package com.hsm.encryption.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsm.encryption.config.HsmProperties;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Batches audit events to Splunk's HTTP Event Collector as newline-delimited JSON.
 * Ported from app/audit/logger.py's SplunkHECBatcher. A no-op (enqueue/start/stop
 * do nothing) when splunk.enabled=false, so callers don't need to branch.
 */
public class SplunkHecBatcher {

    private static final Logger log = LoggerFactory.getLogger("splunk_batcher");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String HOST = resolveHostname();

    private final boolean enabled;
    private final String hecUrl;
    private final String index;
    private final String source;
    private final String sourcetype;
    private final int batchSize;
    private final long flushIntervalSeconds;
    private final HttpClient httpClient;
    private final String authHeader;

    private final Deque<Map<String, Object>> queue = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private ScheduledExecutorService executor;

    public SplunkHecBatcher(HsmProperties.Splunk config, String resolvedToken) {
        this.enabled = config.enabled();
        this.hecUrl = config.hecUrl();
        this.index = config.index();
        this.source = config.source();
        this.sourcetype = config.sourcetype();
        this.batchSize = config.batchSize();
        this.flushIntervalSeconds = config.flushIntervalSeconds();
        this.authHeader = "Splunk " + resolvedToken;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    public void enqueue(Map<String, Object> event) {
        if (!enabled) {
            return;
        }
        lock.lock();
        try {
            queue.addLast(event);
        } finally {
            lock.unlock();
        }
    }

    public void start() {
        if (!enabled) {
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "splunk-hec-flush");
            t.setDaemon(true);
            return t;
        });
        // First flush happens after one interval, matching Python's sleep-then-flush loop.
        executor.scheduleWithFixedDelay(this::flush, flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop() {
        if (!enabled) {
            return;
        }
        if (executor != null) {
            executor.shutdownNow();
        }
        flush(); // one final flush
    }

    private void flush() {
        List<Map<String, Object>> batch;
        lock.lock();
        try {
            if (queue.isEmpty()) {
                return;
            }
            batch = new ArrayList<>();
            for (int i = 0; i < batchSize && !queue.isEmpty(); i++) {
                batch.add(queue.pollFirst());
            }
        } finally {
            lock.unlock();
        }
        if (batch.isEmpty()) {
            return;
        }

        try {
            String payload = buildPayload(batch);
            HttpRequest request = HttpRequest.newBuilder(URI.create(hecUrl))
                    .timeout(Duration.ofSeconds(10))
                    .header("Authorization", authHeader)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 400) {
                throw new IllegalStateException("Splunk HEC returned HTTP " + response.statusCode());
            }
        } catch (Exception e) {
            log.error("splunk_hec_delivery_failed error={} batch_size={}", e.getMessage(), batch.size());
            // Requeue the whole batch at the front, preserving order, for retry on the
            // next tick -- unbounded retry, matching Python (no backoff/retry-count limit).
            lock.lock();
            try {
                for (int i = batch.size() - 1; i >= 0; i--) {
                    queue.addFirst(batch.get(i));
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private String buildPayload(List<Map<String, Object>> batch) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (Map<String, Object> event : batch) {
            Map<String, Object> mutableEvent = new LinkedHashMap<>(event);
            Object epoch = mutableEvent.remove("_epoch");
            double time = epoch instanceof Number n ? n.doubleValue() : System.currentTimeMillis() / 1000.0;

            Map<String, Object> envelope = new LinkedHashMap<>();
            envelope.put("time", time);
            envelope.put("host", HOST);
            envelope.put("source", source);
            envelope.put("sourcetype", sourcetype);
            envelope.put("index", index);
            envelope.put("event", mutableEvent);

            if (!sb.isEmpty()) {
                sb.append("\n");
            }
            sb.append(MAPPER.writeValueAsString(envelope));
        }
        return sb.toString();
    }

    private static String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
