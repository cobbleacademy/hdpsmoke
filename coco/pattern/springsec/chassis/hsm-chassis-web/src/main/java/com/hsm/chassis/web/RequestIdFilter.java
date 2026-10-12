package com.hsm.chassis.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * One request id per request, so a log line, an audit record, an error body and a
 * caller's support ticket can all be matched up. Taken from the caller's header when
 * it sends a sane one, else a fresh UUID; put in MDC for every log line of the
 * request, kept as a request attribute ({@link #of}), and echoed on the response.
 *
 * <p>Configured per service ({@code hsm.chassis.web.request-id.*}), because the
 * services already disagree and their callers depend on what they do today:
 * hsm-file-service uses {@code X-Request-Id} / MDC {@code requestId} (the defaults),
 * hsm-core-service uses {@code X-Correlation-Id} / MDC {@code correlationId}.
 * {@code also-accept} lists further incoming headers to take an id from when the
 * primary one is absent, so ids can flow across services that use different names.
 *
 * <p>Incoming ids are only accepted if they match {@code [A-Za-z0-9._:-]{1,128}}:
 * the id lands in every log line and response, so an arbitrary caller-supplied string
 * would allow log injection.
 */
public class RequestIdFilter extends OncePerRequestFilter {

    public static final String DEFAULT_HEADER = "X-Request-Id";
    public static final String DEFAULT_MDC_KEY = "requestId";
    public static final String ATTRIBUTE = RequestIdFilter.class.getName() + ".id";

    private static final Pattern SAFE = Pattern.compile("[A-Za-z0-9._:-]{1,128}");

    private final String header;
    private final String mdcKey;
    private final List<String> alsoAccept;

    public RequestIdFilter(String header, String mdcKey, List<String> alsoAccept) {
        this.header = header;
        this.mdcKey = mdcKey;
        this.alsoAccept = List.copyOf(alsoAccept);
    }

    public RequestIdFilter() {
        this(DEFAULT_HEADER, DEFAULT_MDC_KEY, List.of());
    }

    public String header() {
        return header;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String id = incoming(request);
        request.setAttribute(ATTRIBUTE, id);
        response.setHeader(header, id);
        MDC.put(mdcKey, id);
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(mdcKey);
        }
    }

    private String incoming(HttpServletRequest request) {
        String primary = request.getHeader(header);
        if (primary != null && SAFE.matcher(primary).matches()) {
            return primary;
        }
        for (String other : alsoAccept) {
            String value = request.getHeader(other);
            if (value != null && SAFE.matcher(value).matches()) {
                return value;
            }
        }
        return UUID.randomUUID().toString();
    }

    /** The id assigned to this request, or null outside a request this filter has seen. */
    public static String of(HttpServletRequest request) {
        Object id = request.getAttribute(ATTRIBUTE);
        return id == null ? null : id.toString();
    }
}
