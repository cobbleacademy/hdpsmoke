package com.hsm.core.config;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import java.security.Security;

/**
 * Registers the Bouncy Castle FIPS Java API (BC-FJA, CMVP certificate #4943,
 * FIPS 140-3, Security Level 1 -- the ceiling for any software-only crypto
 * module) as a JCE security provider, plus its default approved-mode
 * DRBG-backed SecureRandom.
 *
 * <p>Deliberately called from {@code main()} before {@code SpringApplication.run()},
 * not via a {@code @PostConstruct} on a {@code @Configuration} bean: Spring gives
 * no ordering guarantee between unrelated beans' {@code @PostConstruct} methods,
 * and other beans (KekClient, DekCache, ...) construct BC-FIPS ciphers/SecureRandoms
 * eagerly during context refresh. Doing it in {@code main()} guarantees the
 * provider and registrar defaults exist before any Spring bean is created.
 *
 * <p>approved-only mode means only FIPS-approved algorithms/modes are available
 * through the provider -- AES-256-GCM is FIPS-approved (NIST SP 800-38D), so no
 * algorithm changes were needed elsewhere. This covers the DEK-level AES-GCM
 * operations only; the KEK (master key) wrap/unwrap boundary is a separate,
 * higher-assurance layer: Azure Key Vault Managed HSM, validated at FIPS 140-2
 * Level 3 (hardware) -- Level 2+ isn't achievable by any pure-software module.
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
