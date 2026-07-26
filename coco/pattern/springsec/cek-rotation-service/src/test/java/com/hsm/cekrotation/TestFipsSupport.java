package com.hsm.cekrotation;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import java.security.Security;

/**
 * Test-only helper mirroring BouncyCastleFipsConfig: idempotently registers
 * BC-FIPS and its default approved-mode SecureRandom for plain-JUnit tests
 * that don't load the Spring context.
 */
final class TestFipsSupport {

    private TestFipsSupport() {
    }

    static synchronized void ensureReady() {
        BouncyCastleFipsProvider provider = (BouncyCastleFipsProvider) Security.getProvider(BouncyCastleFipsProvider.PROVIDER_NAME);
        if (provider == null) {
            provider = new BouncyCastleFipsProvider();
            Security.addProvider(provider);
            CryptoServicesRegistrar.setSecureRandom(provider.getDefaultSecureRandom());
            CryptoServicesRegistrar.setApprovedOnlyMode(true);
        }
    }
}
