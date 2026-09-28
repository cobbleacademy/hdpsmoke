package com.hsm.fileservice.config;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.crypto.NativeServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Logs, once at startup, whether BC-FIPS is running its native (CPU-accelerated)
 * implementations or pure Java -- so an operator can tell from the log which mode a
 * pod is actually in (chart value bcFips.nativeMode, FILE_SERVICE.md "BC-FIPS native
 * libraries"). Native mode needs an executable, writable directory for the libraries
 * BC-FIPS extracts from its jar (org.bouncycastle.native.loader.install_dir);
 * java mode (org.bouncycastle.native.cpu_variant=java) extracts nothing.
 */
@Component
public class BcFipsStatusLogger {

    private static final Logger log = LoggerFactory.getLogger(BcFipsStatusLogger.class);

    @EventListener(ApplicationReadyEvent.class)
    public void logNativeStatus() {
        NativeServices ns = CryptoServicesRegistrar.getNativeServices();
        log.info("bc_fips_native enabled={} variant={} aes_gcm_native={} install_dir={} status=\"{}\"",
                ns.isEnabled(), ns.getVariant(), ns.hasService(NativeServices.AES_GCM),
                System.getProperty("org.bouncycastle.native.loader.install_dir", "<java.io.tmpdir>"),
                ns.getStatusMessage());
    }
}
