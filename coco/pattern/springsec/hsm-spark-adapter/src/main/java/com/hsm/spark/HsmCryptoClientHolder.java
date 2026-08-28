package com.hsm.spark;

import com.hsm.client.HsmCryptoClient;

/**
 * One {@link HsmCryptoClient} per executor JVM, built lazily on that
 * executor's first UDF invocation -- not at executor boot, and not one per
 * UDF call. Shared by both {@link HsmEncryptUdf} and {@link HsmDecryptUdf} so
 * registering both in one application doesn't spin up two separate
 * connections/caches. Reused across every job in the application for its
 * whole lifetime, so the {@code dek_name} cache stays warm across queries,
 * not just within one.
 *
 * <p>Never explicitly closed -- Spark UDFs have no clean per-application
 * shutdown hook a simple UDF can reliably attach cleanup to; the executor
 * JVM's own teardown reclaims the client. Deliberate, not an oversight.
 */
final class HsmCryptoClientHolder {

    private HsmCryptoClientHolder() {
    }

    private static volatile HsmCryptoClient instance;

    static HsmCryptoClient get() {
        HsmCryptoClient result = instance;
        if (result == null) {
            synchronized (HsmCryptoClientHolder.class) {
                result = instance;
                if (result == null) {
                    result = HsmSparkConfig.buildClient();
                    instance = result;
                }
            }
        }
        return result;
    }
}
