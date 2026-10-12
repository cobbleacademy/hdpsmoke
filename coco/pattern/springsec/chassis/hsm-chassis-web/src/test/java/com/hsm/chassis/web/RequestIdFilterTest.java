package com.hsm.chassis.web;

import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class RequestIdFilterTest {

    private static String run(RequestIdFilter filter, MockHttpServletRequest request, MockHttpServletResponse response,
                              String mdcKey, AtomicReference<String> mdcSeen) throws Exception {
        FilterChain chain = (req, res) -> mdcSeen.set(MDC.get(mdcKey));
        filter.doFilter(request, response, chain);
        return RequestIdFilter.of(request);
    }

    @Test
    void saneIncomingIdIsKeptEchoedAndInMdcDuringTheRequestOnly() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("X-Request-Id", "req-123");
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicReference<String> mdc = new AtomicReference<>();

        String id = run(new RequestIdFilter(), request, response, "requestId", mdc);

        assertThat(id).isEqualTo("req-123");
        assertThat(response.getHeader("X-Request-Id")).isEqualTo("req-123");
        assertThat(mdc.get()).isEqualTo("req-123");
        assertThat(MDC.get("requestId")).as("removed after the request -- threads are pooled").isNull();
    }

    @Test
    void missingOrUnsafeIdIsReplacedWithAUuid() throws Exception {
        for (String unsafe : new String[]{null, "", "has space", "line\nbreak", "x".repeat(129), "<script>"}) {
            MockHttpServletRequest request = new MockHttpServletRequest();
            if (unsafe != null) {
                request.addHeader("X-Request-Id", unsafe);
            }
            MockHttpServletResponse response = new MockHttpServletResponse();

            String id = run(new RequestIdFilter(), request, response, "requestId", new AtomicReference<>());

            assertThatCode(() -> UUID.fromString(id)).as("input %s", unsafe).doesNotThrowAnyException();
            assertThat(response.getHeader("X-Request-Id")).isEqualTo(id);
        }
    }

    @Test
    void configuredHeaderAndMdcKeyAndFallbackHeaders() throws Exception {
        RequestIdFilter filter = new RequestIdFilter("X-Correlation-Id", "correlationId", List.of("X-Request-Id"));

        MockHttpServletRequest fallbackOnly = new MockHttpServletRequest();
        fallbackOnly.addHeader("X-Request-Id", "from-file-service");
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicReference<String> mdc = new AtomicReference<>();
        assertThat(run(filter, fallbackOnly, response, "correlationId", mdc)).isEqualTo("from-file-service");
        assertThat(response.getHeader("X-Correlation-Id")).isEqualTo("from-file-service");
        assertThat(mdc.get()).isEqualTo("from-file-service");

        MockHttpServletRequest both = new MockHttpServletRequest();
        both.addHeader("X-Correlation-Id", "primary");
        both.addHeader("X-Request-Id", "fallback");
        assertThat(run(filter, both, new MockHttpServletResponse(), "correlationId", new AtomicReference<>()))
                .as("primary header wins").isEqualTo("primary");
    }
}
