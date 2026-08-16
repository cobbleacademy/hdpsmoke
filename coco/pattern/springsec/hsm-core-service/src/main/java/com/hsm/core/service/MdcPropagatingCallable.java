package com.hsm.core.service;

import org.slf4j.MDC;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Wraps a Callable so the submitting thread's MDC context (correlationId) is
 * copied onto whichever pooled batchExecutor worker thread actually runs it.
 * MDC is thread-local, so without this every batch item's log lines lose the
 * request's correlation ID the moment work moves off the original HTTP
 * request thread onto the shared executor (see BatchExecutorConfig). Restores
 * the worker thread's prior MDC state afterward, matching
 * CorrelationIdFilter's own finally-block discipline against leaking one
 * task's context into the next task a pooled thread picks up.
 */
final class MdcPropagatingCallable<T> implements Callable<T> {

    private final Callable<T> delegate;
    private final Map<String, String> callerContext;

    private MdcPropagatingCallable(Callable<T> delegate) {
        this.delegate = delegate;
        this.callerContext = MDC.getCopyOfContextMap();
    }

    static <T> Callable<T> wrap(Callable<T> delegate) {
        return new MdcPropagatingCallable<>(delegate);
    }

    @Override
    public T call() throws Exception {
        Map<String, String> previous = MDC.getCopyOfContextMap();
        if (callerContext != null) {
            MDC.setContextMap(callerContext);
        } else {
            MDC.clear();
        }
        try {
            return delegate.call();
        } finally {
            if (previous != null) {
                MDC.setContextMap(previous);
            } else {
                MDC.clear();
            }
        }
    }
}
