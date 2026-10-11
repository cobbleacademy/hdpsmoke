package com.hsm.chassis.fips;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;

/**
 * Logs, once at startup, the BC-FIPS mode a pod is actually running in -- native
 * (CPU-accelerated) or pure Java, and approved-only or not -- so an operator can
 * tell from the log alone (see java/docs/RUNBOOK.md "BC-FIPS native libraries").
 * Same {@code bc_fips_native} line hsm-file-service's own BcFipsStatusLogger has
 * always written, plus {@code approved_only}, now for every service on the chassis.
 */
public class FipsStatusLogger implements ApplicationListener<ApplicationReadyEvent> {

    private static final Logger log = LoggerFactory.getLogger(FipsStatusLogger.class);

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        FipsStatus status = FipsStatus.current();
        log.info("bc_fips_native enabled={} variant={} aes_gcm_native={} install_dir={} status=\"{}\" approved_only={}",
                status.nativeEnabled(), status.nativeVariant(), status.nativeAesGcm(),
                System.getProperty("org.bouncycastle.native.loader.install_dir", "<java.io.tmpdir>"),
                CryptoServicesRegistrar.getNativeServices().getStatusMessage(),
                status.approvedOnly());
    }
}
