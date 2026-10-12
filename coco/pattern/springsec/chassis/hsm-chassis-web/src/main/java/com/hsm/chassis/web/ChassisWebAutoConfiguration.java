package com.hsm.chassis.web;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.env.Environment;

import java.util.Arrays;
import java.util.List;

/**
 * Servlet-web pieces every HTTP service on the chassis gets: the request-id filter
 * (first in the chain, ahead of security, so even an authentication failure's log
 * lines carry the id) and the {@code <api-prefix>/docs} redirect. Each backs off if
 * the service defines its own.
 *
 * <p>Deliberately <b>not</b> here: an error-response format. hsm-core-service
 * ({@code {"detail"}}) and hsm-file-service ({@code {"error_code","message","request_id"}})
 * have different formats that their clients parse, so each keeps its own handler until
 * a client migration is planned (java/docs/COMPATIBILITY_CONTRACT.md C10).
 */
@AutoConfiguration
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
public class ChassisWebAutoConfiguration {

    static final String REQUEST_ID_PREFIX = "hsm.chassis.web.request-id.";
    static final String API_PREFIX_PROPERTY = "hsm.chassis.web.api-prefix";

    @Bean
    @ConditionalOnMissingBean(name = "chassisRequestIdFilter")
    public FilterRegistrationBean<RequestIdFilter> chassisRequestIdFilter(Environment env) {
        RequestIdFilter filter = new RequestIdFilter(
                env.getProperty(REQUEST_ID_PREFIX + "header", RequestIdFilter.DEFAULT_HEADER),
                env.getProperty(REQUEST_ID_PREFIX + "mdc-key", RequestIdFilter.DEFAULT_MDC_KEY),
                list(env.getProperty(REQUEST_ID_PREFIX + "also-accept", "")));
        FilterRegistrationBean<RequestIdFilter> registration = new FilterRegistrationBean<>(filter);
        registration.setName("chassisRequestIdFilter");
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return registration;
    }

    @Bean
    @ConditionalOnMissingBean(ApiDocsEntryController.class)
    @ConditionalOnProperty(prefix = "springdoc.swagger-ui", name = "enabled", havingValue = "true")
    @ConditionalOnProperty(name = API_PREFIX_PROPERTY)
    public ApiDocsEntryController apiDocsEntryController() {
        return new ApiDocsEntryController();
    }

    private static List<String> list(String commaSeparated) {
        return Arrays.stream(commaSeparated.split(",")).map(String::strip).filter(s -> !s.isEmpty()).toList();
    }
}
