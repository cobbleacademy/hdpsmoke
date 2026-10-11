package com.hsm.chassis;

import com.hsm.chassis.fips.FipsGuard;
import com.hsm.chassis.fips.FipsHealthIndicator;
import com.hsm.chassis.fips.FipsStatusLogger;
import com.hsm.chassis.lock.JobLock;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A plain Spring Boot application with nothing but hsm-chassis-core on its
 * classpath picks the chassis up through the real registration files
 * (spring.factories, AutoConfiguration.imports) -- what a service adopting the
 * chassis gets from the dependency alone.
 */
class ChassisBootTest {

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class PlainApp {
    }

    @Test
    void applicationGetsFipsBeansFromTheDependencyAlone() {
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(PlainApp.class)
                .web(WebApplicationType.NONE)
                .run()) {
            assertThat(context.getBeansOfType(FipsGuard.class)).hasSize(1);
            assertThat(context.getBeansOfType(FipsStatusLogger.class)).hasSize(1);
            assertThat(context.getBeansOfType(FipsHealthIndicator.class)).hasSize(1);
            assertThat(context.getBeansOfType(JobLock.class)).as("no DataSource, so no JobLock").isEmpty();
        }
    }
}
