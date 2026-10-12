package com.hsm.crypto;

import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Approved-only mode must hold on threads other than the one that registered BC-FIPS
 * -- request threads, schedulers, Spark tasks. BC-FIPS's per-thread switch alone left
 * every other thread able to use non-approved algorithms; see FipsBootstrap.
 */
class FipsBootstrapTest {

    private static Throwable onAnotherThread(ThrowingRunnable body) throws InterruptedException {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                body.run();
            } catch (Throwable e) {
                failure.set(e);
            }
        });
        t.start();
        t.join();
        return failure.get();
    }

    interface ThrowingRunnable {
        void run() throws Exception;
    }

    @Test
    void approvedOnlyHoldsOnEveryThreadAfterRegister() throws Exception {
        FipsBootstrap.register();

        assertTrue(FipsBootstrap.isApprovedOnlyJvmWide());
        assertEquals("true", System.getProperty(FipsBootstrap.APPROVED_ONLY_PROPERTY));

        Throwable nonApproved = onAnotherThread(() ->
                Cipher.getInstance("Blowfish/ECB/PKCS5Padding", BouncyCastleFipsProvider.PROVIDER_NAME));
        assertTrue(nonApproved instanceof NoSuchAlgorithmException,
                "a non-approved algorithm must be refused on other threads too, got " + nonApproved);

        Throwable approved = onAnotherThread(() -> {
            byte[] dek = DekManager.generateDek();
            DekManager.EncryptResult r = DekManager.encrypt(new byte[]{1, 2, 3}, dek, "app");
            DekManager.decrypt(r.ciphertext(), r.tag(), r.iv(), dek, "app");
        });
        assertEquals(null, approved, "approved AES-GCM must keep working on other threads");
    }
}
