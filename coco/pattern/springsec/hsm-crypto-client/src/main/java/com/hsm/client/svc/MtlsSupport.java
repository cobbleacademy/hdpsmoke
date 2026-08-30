package com.hsm.client.svc;

import com.hsm.client.crypto.TransportWrapper;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

/**
 * Builds the {@link KeyManager}s SvcClient needs to present a client
 * certificate during the TLS handshake for authMode=MTLS -- a purely
 * in-memory PKCS12 keystore assembled from the same bare PEM cert/key pair
 * every other credential in this module uses (mtlsCertPem/mtlsKeyPem), never
 * written to disk. Kept as its own class rather than inlined in SvcClient
 * since it's the one place this module touches java.security.KeyStore at
 * all -- everything else parses PEM directly via TransportWrapper.
 */
final class MtlsSupport {

    private static final char[] IN_MEMORY_KEYSTORE_PASSWORD = "unused".toCharArray();
    private static final String ALIAS = "client";

    private MtlsSupport() {
    }

    static KeyManager[] buildKeyManagers(String certPem, String keyPem) {
        try {
            X509Certificate cert = parseCertificatePem(certPem);
            PrivateKey key = TransportWrapper.parsePrivateKeyPem(keyPem);

            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(null, null);
            keyStore.setKeyEntry(ALIAS, key, IN_MEMORY_KEYSTORE_PASSWORD, new Certificate[]{cert});

            KeyManagerFactory factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            factory.init(keyStore, IN_MEMORY_KEYSTORE_PASSWORD);
            return factory.getKeyManagers();
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to build mTLS client identity from mtlsCertPem/mtlsKeyPem: " + e.getMessage(), e);
        }
    }

    private static X509Certificate parseCertificatePem(String certPem) throws Exception {
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        return (X509Certificate) factory.generateCertificate(
                new ByteArrayInputStream(certPem.getBytes(StandardCharsets.UTF_8)));
    }
}
