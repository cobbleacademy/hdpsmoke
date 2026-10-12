package com.hsm.core.web;

/**
 * This service's request-id contract (java/docs/COMPATIBILITY_CONTRACT.md C11):
 * {@code X-Correlation-Id} in and out, MDC key {@code correlationId}. The filter that
 * enforces it is hsm-chassis-web's RequestIdFilter, configured with these values in
 * application.yml ({@code hsm.chassis.web.request-id.*}); response bodies read the id
 * back from MDC under {@link #MDC_KEY}.
 */
public final class CorrelationId {

    public static final String HEADER = "X-Correlation-Id";
    public static final String MDC_KEY = "correlationId";

    private CorrelationId() {
    }
}
