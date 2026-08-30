package com.hsm.core.security;

import javax.net.ssl.X509TrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * Accepts any client certificate at the TLS handshake -- real identity
 * validation happens afterward, in MtlsAppIdAuthenticationFilter, which
 * compares the presented certificate's fingerprint against what's registered
 * for the claimed X-App-ID. Self-signed certs (this repo's expected mTLS cert
 * shape -- see AUTHORIZATION.md's mTLS section) have no CA to chain-validate
 * against, so this is a deliberate fingerprint-pinned trust model, not a PKI
 * one: the TLS layer's job here is only to prove the caller holds the private
 * key matching whatever certificate it presents, not to decide whether that
 * certificate is one this server recognizes.
 *
 * <p>Loaded by Tomcat via {@code SSLHostConfig.setTrustManagerClassName} --
 * reflection-instantiated with a no-arg constructor, so this class must have
 * one and must be public. See MtlsServerConfig.
 */
public class PermissiveClientTrustManager implements X509TrustManager {

    public PermissiveClientTrustManager() {
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        // Intentionally permissive -- see class javadoc.
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        // Never exercised in this role (server verifying a client), but implemented
        // for interface completeness rather than throwing on an unexpected call path.
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }
}
