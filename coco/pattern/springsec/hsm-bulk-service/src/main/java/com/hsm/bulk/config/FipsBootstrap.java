package com.hsm.bulk.config;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import java.security.Security;

/** Duplicated verbatim from com.hsm.core.config.FipsBootstrap -- see that class's Javadoc for why this runs from main() rather than @PostConstruct. */
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
