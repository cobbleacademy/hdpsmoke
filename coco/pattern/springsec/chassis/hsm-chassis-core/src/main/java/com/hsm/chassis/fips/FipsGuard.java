package com.hsm.chassis.fips;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import java.util.function.Supplier;

/**
 * Startup check: refuses to let the application context finish starting unless
 * BC-FIPS is registered and in approved-only mode. Turns "a service quietly runs
 * non-FIPS crypto" from a silent compliance gap into a pod that never becomes ready.
 *
 * <p>{@code hsm.chassis.fips.enforce=false} downgrades the failure to an ERROR log
 * line -- for local experiments only; never set it in a deployed environment.
 */
public class FipsGuard implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(FipsGuard.class);

    private final Supplier<FipsStatus> status;
    private final boolean enforce;

    public FipsGuard(Supplier<FipsStatus> status, boolean enforce) {
        this.status = status;
        this.enforce = enforce;
    }

    @Override
    public void afterPropertiesSet() {
        String violation = status.get().violation();
        if (violation == null) {
            return;
        }
        if (enforce) {
            throw new IllegalStateException("FIPS check failed: " + violation + " -- refusing to start");
        }
        log.error("fips_check_failed reason=\"{}\" enforce=false -- continuing; never run like this in a deployed environment", violation);
    }
}
