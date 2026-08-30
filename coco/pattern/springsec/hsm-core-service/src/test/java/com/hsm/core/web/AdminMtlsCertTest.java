package com.hsm.core.web;

import com.hsm.core.model.AppRegistration;
import com.hsm.core.repository.AppRegistrationRepository;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.File;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * POST /admin/apps/mtls-cert -- provisioning path for the fourth, optional
 * authentication mechanism (see AUTHORIZATION.md's "mTLS as a fourth,
 * optional authentication mechanism"). Covers the same layer
 * EncryptDecryptIntegrationTest/DekIssueServiceTest do (real HTTP, real
 * demo-profile H2 DB) -- the actual TLS handshake path
 * (MtlsAppIdAuthenticationFilter + MtlsServerConfig's Tomcat customization)
 * is verified separately by a real running instance with server.ssl.*
 * configured, since a RANDOM_PORT SpringBootTest runs plain HTTP by default;
 * this class covers everything on the write/validation side that doesn't
 * need an actual TLS handshake to exercise.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("demo")
class AdminMtlsCertTest {

    @DynamicPropertySource
    static void overrideDatasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:mtlscert-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
    }

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private AppRegistrationRepository appRegistrationRepository;

    private static String certPem;
    private static String certFingerprint;

    /**
     * A real, self-signed X.509 certificate generated via keytool (JDK-bundled,
     * no extra dependency) -- not a hand-built ASN.1 fixture, so this exercises
     * AdminController.setMtlsCert's real CertificateFactory parsing path the
     * same way an actual caller's certificate would.
     */
    @BeforeAll
    static void generateSelfSignedCert() throws Exception {
        File dir = Files.createTempDirectory("mtls-cert-test").toFile();
        File keystoreFile = new File(dir, "test.p12");
        ProcessBuilder genKey = new ProcessBuilder(
                "keytool", "-genkeypair",
                "-alias", "test",
                "-keyalg", "RSA", "-keysize", "2048",
                "-validity", "3650",
                "-dname", "CN=payments-svc-test",
                "-keystore", keystoreFile.getAbsolutePath(),
                "-storetype", "PKCS12",
                "-storepass", "changeit",
                "-keypass", "changeit")
                .redirectErrorStream(true);
        Process p = genKey.start();
        String output = new String(p.getInputStream().readAllBytes());
        int exit = p.waitFor();
        assertEquals(0, exit, "keytool -genkeypair failed: " + output);

        File certFile = new File(dir, "test.pem");
        ProcessBuilder exportCert = new ProcessBuilder(
                "keytool", "-exportcert",
                "-alias", "test",
                "-keystore", keystoreFile.getAbsolutePath(),
                "-storepass", "changeit",
                "-rfc",
                "-file", certFile.getAbsolutePath())
                .redirectErrorStream(true);
        Process p2 = exportCert.start();
        String output2 = new String(p2.getInputStream().readAllBytes());
        int exit2 = p2.waitFor();
        assertEquals(0, exit2, "keytool -exportcert failed: " + output2);

        certPem = Files.readString(certFile.toPath());

        X509Certificate cert = (X509Certificate) CertificateFactory.getInstance("X.509")
                .generateCertificate(new java.io.ByteArrayInputStream(certPem.getBytes()));
        certFingerprint = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(cert.getEncoded()));
    }

    private static HttpHeaders adminHeaders() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set("Authorization", "Bearer demo-token-ops-admin");
        h.set("X-App-ID", "ops-admin");
        return h;
    }

    @Test
    void registersCertAndComputesMatchingFingerprint() {
        String appId = "mtls-target-" + System.nanoTime();
        appRegistrationRepository.save(new AppRegistration(appId, "encrypt,decrypt", "mtls test app", true));

        Map<String, String> body = Map.of("app_id", appId, "cert_pem", certPem);
        ResponseEntity<Map> response = rest.exchange(
                "/api/sensec/hsm/v1/admin/apps/mtls-cert", HttpMethod.POST,
                new HttpEntity<>(body, adminHeaders()), Map.class);

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(appId, response.getBody().get("app_id"));
        assertEquals(certFingerprint, response.getBody().get("fingerprint"));
        assertNotNull(response.getBody().get("updated_at"));

        Optional<AppRegistration> saved = appRegistrationRepository.findById(appId);
        assertTrue(saved.isPresent());
        assertEquals(certFingerprint, saved.get().getMtlsCertFingerprint());
    }

    @Test
    void rejectsUnknownAppId() {
        Map<String, String> body = Map.of("app_id", "no-such-app-" + System.nanoTime(), "cert_pem", certPem);
        ResponseEntity<Map> response = rest.exchange(
                "/api/sensec/hsm/v1/admin/apps/mtls-cert", HttpMethod.POST,
                new HttpEntity<>(body, adminHeaders()), Map.class);

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
    }

    @Test
    void rejectsUnparseableCertPem() {
        String appId = "mtls-target-badcert-" + System.nanoTime();
        appRegistrationRepository.save(new AppRegistration(appId, "encrypt,decrypt", "mtls test app", true));

        Map<String, String> body = Map.of("app_id", appId, "cert_pem", "not a certificate");
        ResponseEntity<Map> response = rest.exchange(
                "/api/sensec/hsm/v1/admin/apps/mtls-cert", HttpMethod.POST,
                new HttpEntity<>(body, adminHeaders()), Map.class);

        assertEquals(HttpStatus.UNPROCESSABLE_CONTENT, response.getStatusCode());
    }

    @Test
    void rejectsCallerWithoutProvisionAppKeysScope() {
        String appId = "mtls-target-forbidden-" + System.nanoTime();
        appRegistrationRepository.save(new AppRegistration(appId, "encrypt,decrypt", "mtls test app", true));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Authorization", "Bearer demo-token-payments-svc"); // encrypt/decrypt scopes only, no provision_app_keys
        headers.set("X-App-ID", "payments-svc");

        Map<String, String> body = Map.of("app_id", appId, "cert_pem", certPem);
        ResponseEntity<Map> response = rest.exchange(
                "/api/sensec/hsm/v1/admin/apps/mtls-cert", HttpMethod.POST,
                new HttpEntity<>(body, headers), Map.class);

        assertEquals(HttpStatus.FORBIDDEN, response.getStatusCode());
    }
}
