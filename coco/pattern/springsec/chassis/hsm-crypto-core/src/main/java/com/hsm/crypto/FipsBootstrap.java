package com.hsm.crypto;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import java.security.Security;

/**
 * Registers the Bouncy Castle FIPS Java API (BC-FJA, CMVP certificate #4943,
 * FIPS 140-3, Security Level 1 -- the ceiling for any software-only crypto
 * module) as a JCE security provider, plus its default approved-mode
 * DRBG-backed SecureRandom, and switches BC-FIPS to approved-only mode.
 *
 * <p>Idempotent. Every class in this library that captures BC-FIPS state at
 * class-load time ({@link DekManager}, {@link IvFactory}, {@link DekCacheCodec})
 * or names the {@code BCFIPS} provider ({@link TransportWrapper}) calls this from
 * its own static initializer, so no caller can reach them in an unregistered
 * state -- in particular, the SecureRandom they capture is always the approved
 * DRBG, never a default {@code SecureRandom} picked up because the class happened
 * to load first. Applications should still call it as early as possible (before
 * any Spring context or other crypto setup), since approved-only mode is
 * process-wide.
 *
 * <p>approved-only mode means only FIPS-approved algorithms/modes are available
 * through the provider -- AES-256-GCM is FIPS-approved (NIST SP 800-38D). This
 * covers the DEK-level AES-GCM operations only; the KEK (master key) wrap/unwrap
 * boundary is a separate, higher-assurance layer: Azure Key Vault Managed HSM,
 * validated at FIPS 140-2 Level 3 (hardware).
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
