package com.hsm.crypto;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import java.security.Security;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Registers the Bouncy Castle FIPS Java API (BC-FJA, CMVP certificate #4943,
 * FIPS 140-3, Security Level 1 -- the ceiling for any software-only crypto
 * module) as a JCE security provider, plus its default approved-mode
 * DRBG-backed SecureRandom, and puts the <b>whole JVM</b> in approved-only mode.
 *
 * <p><b>Approved-only mode must be JVM-wide.</b> BC-FIPS's
 * {@code CryptoServicesRegistrar.setApprovedOnlyMode(true)} only affects the
 * <em>calling thread</em>: every other thread -- HTTP request threads, schedulers,
 * Spark tasks -- stays in general mode and can use non-approved algorithms.
 * (Verified: after that call alone, Blowfish via BCFIPS is refused on the calling
 * thread and allowed on any other.) JVM-wide mode comes only from the system
 * property {@value #APPROVED_ONLY_PROPERTY}, which BC-FIPS reads once, when it
 * initializes -- so {@link #register()} sets it before touching any BC-FIPS class.
 * If something initialized BC-FIPS earlier the property has no effect;
 * {@link #isApprovedOnlyJvmWide()} detects that (hsm-chassis-core's startup check
 * refuses to start), and {@code -Dorg.bouncycastle.fips.approved_only=true} on the
 * JVM command line guarantees it regardless of class-loading order.
 *
 * <p>Idempotent. Every class in this library that captures BC-FIPS state at
 * class-load time ({@link DekManager}, {@link IvFactory}, {@link DekCacheCodec})
 * or names the {@code BCFIPS} provider ({@link TransportWrapper}) calls this from
 * its own static initializer, so no caller can reach them in an unregistered
 * state. Applications should still call it as early as possible (hsm-chassis-core
 * does, before any Spring bean exists).
 *
 * <p>approved-only mode means only FIPS-approved algorithms/modes are available
 * through the provider -- AES-256-GCM is FIPS-approved (NIST SP 800-38D). This
 * covers the DEK-level AES-GCM operations only; the KEK (master key) wrap/unwrap
 * boundary is a separate, higher-assurance layer: Azure Key Vault Managed HSM,
 * validated at FIPS 140-2 Level 3 (hardware).
 */
public final class FipsBootstrap {

    /** BC-FIPS's JVM-wide approved-only switch, read once when BC-FIPS initializes. */
    public static final String APPROVED_ONLY_PROPERTY = "org.bouncycastle.fips.approved_only";

    private FipsBootstrap() {
    }

    public static synchronized void register() {
        // First, before any BC-FIPS class initializes -- see the class javadoc.
        System.setProperty(APPROVED_ONLY_PROPERTY, "true");

        BouncyCastleFipsProvider provider = (BouncyCastleFipsProvider) Security.getProvider(BouncyCastleFipsProvider.PROVIDER_NAME);
        if (provider == null) {
            provider = new BouncyCastleFipsProvider();
            Security.addProvider(provider);
            CryptoServicesRegistrar.setSecureRandom(provider.getDefaultSecureRandom());
            // Covers this thread even if the JVM-wide property arrived too late.
            CryptoServicesRegistrar.setApprovedOnlyMode(true);
        }
    }

    /**
     * Whether approved-only mode holds for <em>every</em> thread, checked from a
     * fresh thread -- the calling thread can be approved-only on its own (see the
     * class javadoc), so asking it would prove nothing.
     */
    public static boolean isApprovedOnlyJvmWide() {
        AtomicBoolean approved = new AtomicBoolean(false);
        Thread probe = new Thread(() -> approved.set(CryptoServicesRegistrar.isInApprovedOnlyMode()), "fips-approved-only-probe");
        probe.start();
        try {
            probe.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        return approved.get();
    }
}
