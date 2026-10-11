package com.hsm.chassis.fips;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

import java.util.function.Supplier;

/**
 * {@code fips} health component: DOWN if BC-FIPS is unregistered or out of
 * approved-only mode, with the native-acceleration details either way. Only
 * created when the service has actuator health on its classpath.
 */
public class FipsHealthIndicator implements HealthIndicator {

    private final Supplier<FipsStatus> status;

    public FipsHealthIndicator(Supplier<FipsStatus> status) {
        this.status = status;
    }

    @Override
    public Health health() {
        FipsStatus s = status.get();
        Health.Builder builder = s.violation() == null ? Health.up() : Health.down().withDetail("reason", s.violation());
        return builder
                .withDetail("approvedOnly", s.approvedOnly())
                .withDetail("nativeEnabled", s.nativeEnabled())
                .withDetail("nativeVariant", s.nativeVariant())
                .withDetail("nativeAesGcm", s.nativeAesGcm())
                .build();
    }
}
