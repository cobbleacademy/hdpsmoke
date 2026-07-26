package com.hsm.encryption.crypto;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import java.security.Security;

/**
 * Test-only helper mirroring BouncyCastleConfig: idempotently registers BC-FIPS
 * and its default approved-mode SecureRandom. Needed by plain-JUnit unit tests
 * (DekManagerTest, IvFactoryTest) that exercise the crypto package directly,
 * without loading the Spring context that would otherwise do this via
 * com.hsm.encryption.config.BouncyCastleConfig.
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
