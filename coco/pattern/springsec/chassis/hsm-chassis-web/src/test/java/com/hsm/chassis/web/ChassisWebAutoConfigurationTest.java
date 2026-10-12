package com.hsm.chassis.web;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.core.Ordered;

import static org.assertj.core.api.Assertions.assertThat;

class ChassisWebAutoConfigurationTest {

    private final WebApplicationContextRunner web = new WebApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ChassisWebAutoConfiguration.class));

    @Test
    @SuppressWarnings("unchecked")
    void requestIdFilterIsRegisteredFirstWithDefaults() {
        web.run(context -> {
            FilterRegistrationBean<RequestIdFilter> reg = context.getBean("chassisRequestIdFilter", FilterRegistrationBean.class);
            assertThat(reg.getOrder()).isEqualTo(Ordered.HIGHEST_PRECEDENCE);
            assertThat(reg.getFilter().header()).isEqualTo("X-Request-Id");
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void requestIdHeaderIsConfigurable() {
        web.withPropertyValues("hsm.chassis.web.request-id.header=X-Correlation-Id")
                .run(context -> assertThat(context.getBean("chassisRequestIdFilter", FilterRegistrationBean.class)
                        .getFilter()).extracting("header").isEqualTo("X-Correlation-Id"));
    }

    @Test
    void docsRedirectOnlyWithSwaggerUiEnabledAndAPrefix() {
        web.run(context -> assertThat(context).doesNotHaveBean(ApiDocsEntryController.class));
        web.withPropertyValues("springdoc.swagger-ui.enabled=true")
                .run(context -> assertThat(context).doesNotHaveBean(ApiDocsEntryController.class));
        web.withPropertyValues("hsm.chassis.web.api-prefix=/api/x/v1")
                .run(context -> assertThat(context).doesNotHaveBean(ApiDocsEntryController.class));
        web.withPropertyValues("springdoc.swagger-ui.enabled=true", "hsm.chassis.web.api-prefix=/api/x/v1")
                .run(context -> assertThat(context).hasSingleBean(ApiDocsEntryController.class));
    }

    @Test
    void nothingInANonWebApplication() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(ChassisWebAutoConfiguration.class))
                .run(context -> assertThat(context).doesNotHaveBean("chassisRequestIdFilter"));
    }
}
