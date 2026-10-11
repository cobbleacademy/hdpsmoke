package com.hsm.chassis.fips;

import com.hsm.crypto.FipsBootstrap;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * Registers BC-FIPS (approved-only mode, approved DRBG) the moment a Spring Boot
 * application starts preparing its environment -- before any bean definition is
 * read or any bean is created, and equally under {@code @SpringBootTest}, which
 * bypasses {@code main()}. This replaces the per-service
 * {@code static { FipsBootstrap.register(); }} blocks on application classes:
 * a service gets FIPS by having hsm-chassis-core on its classpath, and can no
 * longer forget it.
 *
 * <p>Registered in META-INF/spring.factories. {@link FipsBootstrap#register()} is
 * idempotent, so a service that still calls it itself is unaffected.
 */
public class FipsEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        FipsBootstrap.register();
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
