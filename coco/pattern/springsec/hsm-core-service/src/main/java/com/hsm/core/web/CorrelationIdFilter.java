package com.hsm.core.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Assigns one correlation ID per request -- reused from an incoming
 * X-Correlation-Id header if the caller (e.g. a gateway that already mints its
 * own upstream trace ID) supplied one, otherwise generated fresh. Placed in
 * MDC before the rest of the filter chain runs, so every log line for this
 * request picks it up automatically via logging.pattern.level
 * (application.yml) without each call site having to thread it through
 * explicitly, and echoed back on the response so the caller can correlate
 * their own logs to this service's.
 *
 * <p>Registered ahead of JwtAppIdAuthenticationFilter (see SecurityConfig) so
 * even an authentication failure's audit/log entries carry a correlation ID.
 * MDC is thread-bound, not request-bound, so the ID is always removed in a
 * finally block -- leaving it set would leak into whatever the next request
 * this (pooled) thread happens to serve.
 */
public class CorrelationIdFilter extends OncePerRequestFilter {

    static final String HEADER = "X-Correlation-Id";

    /** Public so response-envelope code (EncryptResponse/DecryptResponse's correlationId field) can read the same key without duplicating the literal. */
    public static final String MDC_KEY = "correlationId";

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        String incoming = request.getHeader(HEADER);
        String correlationId = incoming != null && !incoming.isBlank() ? incoming.trim() : UUID.randomUUID().toString();
        MDC.put(MDC_KEY, correlationId);
        response.setHeader(HEADER, correlationId);
        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove(MDC_KEY);
        }
    }
}
