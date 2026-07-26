package com.hsm.encryption.audit;

import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/** In-memory ring buffer of the most recent 200 audit events, backing GET /demo/audit-log. */
@Component
public class RecentEventsBuffer {

    private static final int MAX_SIZE = 200;

    private final Deque<Map<String, Object>> events = new ArrayDeque<>();

    public synchronized void add(Map<String, Object> event) {
        if (events.size() >= MAX_SIZE) {
            events.removeFirst();
        }
        events.addLast(event);
    }

    /** Most-recent-first, capped at limit. */
    public synchronized List<Map<String, Object>> recent(int limit) {
        List<Map<String, Object>> all = new ArrayList<>(events);
        Collections.reverse(all);
        if (limit >= 0 && limit < all.size()) {
            return new ArrayList<>(all.subList(0, limit));
        }
        return all;
    }
}
