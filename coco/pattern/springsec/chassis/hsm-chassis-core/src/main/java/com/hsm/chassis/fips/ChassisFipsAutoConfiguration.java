package com.hsm.chassis.fips;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * FIPS for every service on the chassis: the startup check, the startup status
 * line, and (with actuator) a {@code fips} health component. Registration itself
 * happens earlier, in {@link FipsEnvironmentPostProcessor}. Each bean backs off if
 * the service defines its own.
 */
@AutoConfiguration
public class ChassisFipsAutoConfiguration {

    static final String ENFORCE_PROPERTY = "hsm.chassis.fips.enforce";

    @Bean
    @ConditionalOnMissingBean
    public FipsGuard fipsGuard(Environment environment) {
        return new FipsGuard(FipsStatus::current, environment.getProperty(ENFORCE_PROPERTY, Boolean.class, true));
    }

    @Bean
    @ConditionalOnMissingBean
    public FipsStatusLogger fipsStatusLogger() {
        return new FipsStatusLogger();
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.boot.health.contributor.HealthIndicator")
    static class FipsHealthConfiguration {

        @Bean
        @ConditionalOnMissingBean(name = "fipsHealthIndicator")
        public FipsHealthIndicator fipsHealthIndicator() {
            return new FipsHealthIndicator(FipsStatus::current);
        }
    }
}
