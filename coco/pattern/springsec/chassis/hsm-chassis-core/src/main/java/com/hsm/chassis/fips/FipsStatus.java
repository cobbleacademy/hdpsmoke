package com.hsm.chassis.fips;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.crypto.NativeServices;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;

import java.security.Security;

/**
 * Snapshot of this JVM's BC-FIPS state -- what the startup check enforces, the
 * startup log line reports, and the health endpoint exposes.
 *
 * @param providerRegistered the {@code BCFIPS} JCE provider is installed
 * @param approvedOnly       BC-FIPS is in approved-only mode (non-approved algorithms refused)
 * @param nativeEnabled      BC-FIPS is running its native (CPU-accelerated) implementations
 * @param nativeVariant      the native variant in use, or the reason there is none
 * @param nativeAesGcm       AES-GCM specifically is native-accelerated
 */
public record FipsStatus(boolean providerRegistered, boolean approvedOnly,
                         boolean nativeEnabled, String nativeVariant, boolean nativeAesGcm) {

    public static FipsStatus current() {
        NativeServices nativeServices = CryptoServicesRegistrar.getNativeServices();
        return new FipsStatus(
                Security.getProvider(BouncyCastleFipsProvider.PROVIDER_NAME) != null,
                CryptoServicesRegistrar.isInApprovedOnlyMode(),
                nativeServices.isEnabled(),
                String.valueOf(nativeServices.getVariant()),
                nativeServices.hasService(NativeServices.AES_GCM));
    }

    /** What makes this JVM non-compliant, or null if nothing does. */
    String violation() {
        if (!providerRegistered) {
            return "BC-FIPS provider (" + BouncyCastleFipsProvider.PROVIDER_NAME + ") is not registered";
        }
        if (!approvedOnly) {
            return "BC-FIPS is not in approved-only mode";
        }
        return null;
    }
}
