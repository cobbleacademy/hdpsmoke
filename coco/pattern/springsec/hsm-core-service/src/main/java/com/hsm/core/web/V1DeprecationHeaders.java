package com.hsm.core.web;

import com.hsm.core.config.HsmProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Announces v1's retirement on every v1 response once the dates are set (both off by
 * default -- v1 responses are unchanged until then):
 * <ul>
 *   <li>{@code hsm.service.api-v1-deprecated-at} (ISO date) -- {@code Deprecation: @<epoch>}
 *       (RFC 9745) plus {@code Link: <api-v2-prefix>; rel="successor-version"}.</li>
 *   <li>{@code hsm.service.api-v1-sunset-at} (ISO date) -- {@code Sunset: <HTTP-date>}
 *       (RFC 8594), the date v1 stops being served.</li>
 * </ul>
 * Runs ahead of the security filters, so 401/403 responses carry the headers too.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class V1DeprecationHeaders extends OncePerRequestFilter {

    private final String v1Prefix;
    private final String deprecation;
    private final String successor;
    private final String sunset;

    public V1DeprecationHeaders(HsmProperties properties) {
        HsmProperties.Service service = properties.service();
        this.v1Prefix = service.apiV1Prefix() + "/";
        LocalDate deprecatedAt = parse(service.apiV1DeprecatedAt());
        LocalDate sunsetAt = parse(service.apiV1SunsetAt());
        this.deprecation = deprecatedAt == null ? null : "@" + deprecatedAt.atStartOfDay(ZoneOffset.UTC).toEpochSecond();
        this.successor = deprecatedAt == null ? null : "<" + service.apiV2Prefix() + ">; rel=\"successor-version\"";
        this.sunset = sunsetAt == null ? null
                : DateTimeFormatter.RFC_1123_DATE_TIME.format(sunsetAt.atStartOfDay(ZoneOffset.UTC));
    }

    private static LocalDate parse(String date) {
        return date == null || date.isBlank() ? null : LocalDate.parse(date.trim());
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return (deprecation == null && sunset == null) || !request.getRequestURI().startsWith(v1Prefix);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if (deprecation != null) {
            response.setHeader("Deprecation", deprecation);
            response.addHeader("Link", successor);
        }
        if (sunset != null) {
            response.setHeader("Sunset", sunset);
        }
        chain.doFilter(request, response);
    }
}
