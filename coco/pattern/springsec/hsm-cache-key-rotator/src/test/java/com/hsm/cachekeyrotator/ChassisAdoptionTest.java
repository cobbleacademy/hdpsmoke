package com.hsm.cachekeyrotator;

import com.hsm.chassis.fips.FipsGuard;
import com.hsm.chassis.fips.FipsStatusLogger;
import com.hsm.chassis.lock.JobLock;
import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The rotator's own Spring context, started for real, on the chassis: BC-FIPS is in
 * approved-only mode without the application class registering it, the chassis
 * startup check and status logger are present, and no JobLock is created (the
 * rotator has no database; it stays a single pod via replicaCount 1 + Recreate).
 * RotationRunner is replaced so startup doesn't reach for Key Vault.
 */
@SpringBootTest
class ChassisAdoptionTest {

    @MockitoBean
    private RotationRunner rotationRunner;

    @Autowired
    private ApplicationContext context;

    @Test
    void rotatorStartsOnTheChassis() {
        assertTrue(CryptoServicesRegistrar.isInApprovedOnlyMode());
        assertEquals(1, context.getBeansOfType(FipsGuard.class).size());
        assertEquals(1, context.getBeansOfType(FipsStatusLogger.class).size());
        assertEquals(0, context.getBeansOfType(JobLock.class).size());
    }
}
