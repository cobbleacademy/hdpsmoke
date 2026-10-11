package com.hsm.chassis.fips;

import org.junit.jupiter.api.Test;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChassisFipsAutoConfigurationTest {

    private static final FipsStatus COMPLIANT = new FipsStatus(true, true, false, "java", false);

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ChassisFipsAutoConfiguration.class));

    @Test
    void postProcessorIsRegisteredAndLeavesBcFipsInApprovedOnlyMode() throws Exception {
        // Read the registration rather than SpringFactoriesLoader.load(): that would also
        // instantiate Spring Boot's own post-processors, which need constructor arguments.
        Properties registrations = new Properties();
        for (URL url : Collections.list(getClass().getClassLoader().getResources("META-INF/spring.factories"))) {
            try (InputStream in = url.openStream()) {
                Properties p = new Properties();
                p.load(in);
                String value = p.getProperty(EnvironmentPostProcessor.class.getName());
                if (value != null && value.contains(FipsEnvironmentPostProcessor.class.getName())) {
                    registrations.putAll(p);
                }
            }
        }
        assertThat(registrations).as("FipsEnvironmentPostProcessor registered in META-INF/spring.factories").isNotEmpty();

        new FipsEnvironmentPostProcessor().postProcessEnvironment(null, null);

        FipsStatus status = FipsStatus.current();
        assertThat(status.providerRegistered()).isTrue();
        assertThat(status.approvedOnly()).isTrue();
    }

    @Test
    void contextStartsWithGuardLoggerAndHealthWhenFipsIsActive() {
        new FipsEnvironmentPostProcessor().postProcessEnvironment(null, null);
        runner.run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(FipsGuard.class).hasSingleBean(FipsStatusLogger.class);
            Health health = context.getBean(FipsHealthIndicator.class).health();
            assertThat(health.getStatus()).isEqualTo(Status.UP);
            assertThat(health.getDetails()).containsEntry("approvedOnly", true).containsKey("nativeEnabled");
        });
    }

    @Test
    void noHealthComponentWithoutActuatorHealthOnTheClasspath() {
        runner.withClassLoader(new FilteredClassLoader(HealthIndicator.class))
                .run(context -> assertThat(context).hasNotFailed().doesNotHaveBean(FipsHealthIndicator.class));
    }

    @Test
    void serviceDefinedGuardIsKept() {
        FipsGuard own = new FipsGuard(() -> COMPLIANT, true);
        runner.withBean(FipsGuard.class, () -> own)
                .run(context -> assertThat(context).getBean(FipsGuard.class).isSameAs(own));
    }

    @Test
    void guardRefusesToStartOutsideApprovedOnlyMode() {
        FipsGuard guard = new FipsGuard(() -> new FipsStatus(true, false, false, "java", false), true);
        assertThatThrownBy(guard::afterPropertiesSet)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("approved-only");
    }

    @Test
    void guardRefusesToStartWithoutTheProvider() {
        FipsGuard guard = new FipsGuard(() -> new FipsStatus(false, false, false, "java", false), true);
        assertThatThrownBy(guard::afterPropertiesSet).hasMessageContaining("not registered");
    }

    @Test
    void guardOnlyLogsWhenEnforcementIsOff() {
        FipsGuard guard = new FipsGuard(() -> new FipsStatus(true, false, false, "java", false), false);
        assertThatCode(guard::afterPropertiesSet).doesNotThrowAnyException();
    }

    @Test
    void enforcementCanBeTurnedOffByProperty() {
        runner.withPropertyValues(ChassisFipsAutoConfiguration.ENFORCE_PROPERTY + "=false")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    void healthIsDownOutsideApprovedOnlyMode() {
        Health health = new FipsHealthIndicator(() -> new FipsStatus(true, false, false, "java", false)).health();
        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsKey("reason");
    }
}
