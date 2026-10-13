package com.hsm.chassis.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
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
 * Moved from hsm-core-service's SplunkHecBatcher (OBS-2 step 2) with its behaviour and
 * payload unchanged; created only when HEC delivery is enabled.
 *
 * <p>Delivery is best-effort and must never threaten the service itself -- every event
 * is also on stdout (the audit.json stream), which stays the complete record:
 * <ul>
 *   <li>The in-memory queue is bounded ({@code max-queue-size}); when full, the
 *       <em>oldest</em> event is dropped and counted, and the count is logged
 *       ({@code splunk_hec_events_dropped}) once per flush. A long Splunk outage costs
 *       HEC delivery of old events, not the pod's memory.</li>
 *   <li>Each flush sends batches until the queue is empty or a send fails, so delivery
 *       keeps up with any event rate rather than one batch per interval.</li>
 *   <li>A failed batch is put back at the front (order preserved) and retried on the
 *       next flush, still subject to the queue bound.</li>
 *   <li>On shutdown, sending continues for up to {@code shutdown-flush-seconds}.</li>
 * </ul>
 */
public class SplunkHecSink implements AuditSink {

    /**
     * HEC delivery settings. {@code source} names the emitting release in Splunk;
     * {@code maxQueueSize} bounds memory (oldest dropped beyond it, still on stdout);
     * {@code shutdownFlushSeconds} bounds how long shutdown keeps sending.
     */
    public record Settings(String hecUrl, String index, String source, String sourcetype,
                           int batchSize, int flushIntervalSeconds, int maxQueueSize, int shutdownFlushSeconds) {
    }


    private static final Logger log = LoggerFactory.getLogger("splunk_batcher");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String HOST = resolveHostname();

    private final String hecUrl;
    private final String index;
    private final String source;
    private final String sourcetype;
    private final int batchSize;
    private final long flushIntervalSeconds;
    private final int maxQueueSize;
    private final long shutdownFlushSeconds;
    private final HttpClient httpClient;
    private final String authHeader;

    private final Deque<Map<String, Object>> queue = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private long droppedTotal;
    private long droppedSinceLastReport;
    private ScheduledExecutorService executor;

    public SplunkHecSink(Settings config, String resolvedToken) {
        this.hecUrl = config.hecUrl();
        this.index = config.index();
        this.source = config.source();
        this.sourcetype = config.sourcetype();
        this.batchSize = config.batchSize();
        this.flushIntervalSeconds = config.flushIntervalSeconds();
        this.maxQueueSize = Math.max(1, config.maxQueueSize());
        this.shutdownFlushSeconds = Math.max(0, config.shutdownFlushSeconds());
        this.authHeader = "Splunk " + resolvedToken;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    @Override
    public void accept(AuditRecord record) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("event_type", record.eventType());
        event.put("_epoch", record.epochSeconds());
        event.putAll(record.fields());
        enqueue(event);
    }

    /** Queues one HEC event map ({@code _epoch} becomes the HEC {@code time}). */
    void enqueue(Map<String, Object> event) {
        lock.lock();
        try {
            queue.addLast(event);
            trimToBound();
        } finally {
            lock.unlock();
        }
    }

    /** Caller holds {@link #lock}. Drops the oldest events beyond the bound. */
    private void trimToBound() {
        while (queue.size() > maxQueueSize) {
            queue.pollFirst();
            droppedTotal++;
            droppedSinceLastReport++;
        }
    }

    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "splunk-hec-flush");
            t.setDaemon(true);
            return t;
        });
        // First flush happens after one interval, matching Python's sleep-then-flush loop.
        executor.scheduleWithFixedDelay(this::flush, flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS);
    }

    public void stop() {
        if (executor != null) {
            executor.shutdownNow();
        }
        // Keep sending what's queued, but never hold up shutdown past the budget.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(shutdownFlushSeconds);
        while (System.nanoTime() < deadline && sendNextBatch()) {
            // loop until empty, a failed send, or the deadline
        }
        reportDrops();
        int left = queueSize();
        if (left > 0) {
            log.warn("splunk_hec_shutdown_undelivered count={} -- still on stdout (audit.json)", left);
        }
    }

    /** One scheduled tick: send until the queue is empty or a send fails. */
    void flush() {
        while (sendNextBatch()) {
            // keep draining
        }
        reportDrops();
    }

    private void reportDrops() {
        long dropped;
        lock.lock();
        try {
            dropped = droppedSinceLastReport;
            droppedSinceLastReport = 0;
        } finally {
            lock.unlock();
        }
        if (dropped > 0) {
            log.warn("splunk_hec_events_dropped count={} max_queue_size={} -- oldest dropped; still on stdout (audit.json)",
                    dropped, maxQueueSize);
        }
    }

    /** Sends one batch. True if a batch was delivered (so there may be more); false if the queue was empty or the send failed. */
    private boolean sendNextBatch() {
        List<Map<String, Object>> batch;
        lock.lock();
        try {
            if (queue.isEmpty()) {
                return false;
            }
            batch = new ArrayList<>();
            for (int i = 0; i < batchSize && !queue.isEmpty(); i++) {
                batch.add(queue.pollFirst());
            }
        } finally {
            lock.unlock();
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
            return true;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.error("splunk_hec_delivery_failed error={} batch_size={}", e.getMessage(), batch.size());
            // Requeue the whole batch at the front, preserving order, for retry on the
            // next tick -- still subject to the queue bound (oldest dropped first).
            lock.lock();
            try {
                for (int i = batch.size() - 1; i >= 0; i--) {
                    queue.addFirst(batch.get(i));
                }
                trimToBound();
            } finally {
                lock.unlock();
            }
            return false;
        }
    }

    int queueSize() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    long droppedTotal() {
        lock.lock();
        try {
            return droppedTotal;
        } finally {
            lock.unlock();
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
