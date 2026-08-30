package com.hsm.core.security;

import org.apache.catalina.connector.Connector;
import org.apache.tomcat.util.net.SSLHostConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.tomcat.TomcatConnectorCustomizer;
import org.springframework.boot.tomcat.servlet.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures the embedded Tomcat connector to request (not require -- "want",
 * Tomcat's {@code certificateVerification=optional}) a client certificate on
 * every TLS handshake when hsm.security.mtls-enabled=true, using
 * {@link PermissiveClientTrustManager} so self-signed client certs (this
 * repo's expected mTLS shape, with no CA to chain-validate against) don't
 * fail the handshake outright -- real identity validation happens afterward
 * in {@link MtlsAppIdAuthenticationFilter}, not at the TLS layer.
 *
 * <p>Requires {@code server.ssl.*} to already be configured (key-store,
 * key-store-password, etc.) -- this class only adjusts the client-auth side
 * of whatever server identity Spring Boot's own standard SSL bootstrapping
 * already set up; it never touches the server's own presented certificate.
 * "want" rather than "need"/"required" is what makes mTLS optional per
 * caller: a request with no client certificate at all completes the TLS
 * handshake exactly as before and falls through to
 * JwtAppIdAuthenticationFilter unchanged.
 *
 * <p>Only registered when hsm.security.mtls-enabled=true -- when false (the
 * default), this bean doesn't exist and the connector is left exactly as
 * Spring Boot's own {@code server.ssl.*} properties configure it.
 */
@Configuration
@ConditionalOnProperty(prefix = "hsm.security", name = "mtls-enabled", havingValue = "true")
public class MtlsServerConfig {

    @Bean
    public WebServerFactoryCustomizer<TomcatServletWebServerFactory> mtlsClientAuthCustomizer() {
        return factory -> factory.addConnectorCustomizers(mtlsConnectorCustomizer());
    }

    private static TomcatConnectorCustomizer mtlsConnectorCustomizer() {
        return (Connector connector) -> {
            for (SSLHostConfig hostConfig : connector.findSslHostConfigs()) {
                hostConfig.setCertificateVerification("optional");
                hostConfig.setTrustManagerClassName(PermissiveClientTrustManager.class.getName());
            }
        };
    }
}
