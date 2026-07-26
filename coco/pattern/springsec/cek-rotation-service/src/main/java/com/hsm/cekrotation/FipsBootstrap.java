package com.hsm.cekrotation;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import java.security.Security;

/**
 * Registers the Bouncy Castle FIPS Java API (BC-FJA, CMVP certificate #4943,
 * FIPS 140-3, Security Level 1) as a JCE security provider, plus its default
 * approved-mode DRBG-backed SecureRandom. See hsm-encryption-service's
 * FipsBootstrap for why this runs from a static initializer rather than a
 * {@code @PostConstruct} bean hook.
 */
public final class FipsBootstrap {

    private FipsBootstrap() {
    }

    public static synchronized void register() {
        BouncyCastleFipsProvider provider = (BouncyCastleFipsProvider) Security.getProvider(BouncyCastleFipsProvider.PROVIDER_NAME);
        if (provider == null) {
            provider = new BouncyCastleFipsProvider();
            Security.addProvider(provider);
            CryptoServicesRegistrar.setSecureRandom(provider.getDefaultSecureRandom());
            CryptoServicesRegistrar.setApprovedOnlyMode(true);
        }
    }
}
