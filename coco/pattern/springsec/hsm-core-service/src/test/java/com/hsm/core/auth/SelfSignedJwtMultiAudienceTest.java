package com.hsm.core.auth;

import com.hsm.core.model.AppRegistration;
import com.hsm.core.repository.AppRegistrationRepository;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
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

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.util.Base64;
import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * End-to-end coverage (real HTTP, real demo-profile H2 DB, real filter chain --
 * SelfIssuedRoutingJwtValidator -> SelfSignedAppKeyJwtValidator) for the
 * comma-separated multi-value hsm.jwt.audience support added to
 * RsaJwtValidator/SelfSignedAppKeyJwtValidator/SelfIssuedRoutingJwtValidator.
 * A real deployment might set hsm.jwt.audience to something like
 * "https://sts.windows.net/tenant/aud,api://hsm-core-service" so both an Azure
 * AD SPN-acquired token AND a SELF_SIGNED_JWT caller's own literal "hsm-core-
 * service" aud validate against the same configured value -- this proves the
 * SELF_SIGNED_JWT side of that specifically, since RsaJwtValidatorTest already
 * covers the AZURE_AD/RsaJwtValidator side as a pure unit test.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("demo")
class SelfSignedJwtMultiAudienceTest {

    private static final String MULTI_VALUE_AUDIENCE =
            "https://sts.windows.net/tenant-guid/aud,api://hsm-core-service,hsm-core-service";

    @DynamicPropertySource
    static void overrideDatasourceAndJwtAudience(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:mem:ssjwt-multi-aud-" + System.nanoTime() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        // Simulates an operator who has already widened hsm.jwt.audience to also
        // accept Azure AD tokens (see RsaJwtValidatorTest) -- SELF_SIGNED_JWT callers
        // must keep working unaffected by that change.
        registry.add("hsm.jwt.audience", () -> MULTI_VALUE_AUDIENCE);
    }

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private AppRegistrationRepository appRegistrationRepository;

    private static String signedTokenFor(String appId, RSAPrivateKey privateKey, String audience) throws Exception {
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject(appId)
                .issuer(appId)
                .audience(audience)
                .issueTime(new Date())
                .expirationTime(new Date(System.currentTimeMillis() + 120_000))
                .build();
        SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.RS256), claims);
        jwt.sign(new RSASSASigner(privateKey));
        return jwt.serialize();
    }

    private static String pemEncodePublicKey(java.security.PublicKey key) {
        String base64 = Base64.getEncoder().encodeToString(key.getEncoded());
        StringBuilder pem = new StringBuilder("-----BEGIN PUBLIC KEY-----\n");
        for (int i = 0; i < base64.length(); i += 64) {
            pem.append(base64, i, Math.min(i + 64, base64.length())).append('\n');
        }
        return pem.append("-----END PUBLIC KEY-----\n").toString();
    }

    /** appId's own literal "hsm-core-service" aud -- SelfSignedJwtTokenProvider's default when
     * client.svc.self-signed-audience is left unset -- must still be one of the accepted values. */
    @Test
    void acceptsSelfSignedJwtWithDefaultAudience_evenWhenServerAudienceIsAMultiValueList() throws Exception {
        assertSelfSignedTokenAccepted("hsm-core-service");
    }

    /** A caller that explicitly configured client.svc.self-signed-audience to one of the
     * other listed values (e.g. matching what an operator added for Azure AD) must also work. */
    @Test
    void acceptsSelfSignedJwtWithNonDefaultListedAudience() throws Exception {
        assertSelfSignedTokenAccepted("api://hsm-core-service");
    }

    @Test
    void rejectsSelfSignedJwtWithAudienceNotInList() throws Exception {
        KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        String appId = "ssjwt-reject-" + System.nanoTime();
        appRegistrationRepository.save(new AppRegistration(appId, "encrypt,decrypt", "ssjwt multi-aud reject test", true));
        appRegistrationRepository.findById(appId).ifPresent(row -> row.setSigningPublicKeyPem(pemEncodePublicKey(keyPair.getPublic())));

        String token = signedTokenFor(appId, (RSAPrivateKey) keyPair.getPrivate(), "not-in-the-configured-list");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Authorization", "Bearer " + token);
        headers.set("X-App-ID", appId);
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(Map.of("plaintext", "should be rejected"), headers);

        ResponseEntity<Map> resp = rest.postForEntity("/api/sensec/hsm/v1/encrypt", req, Map.class);

        assertEquals(HttpStatus.UNAUTHORIZED, resp.getStatusCode());
    }

    private void assertSelfSignedTokenAccepted(String audience) throws Exception {
        KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        String appId = "ssjwt-multi-aud-" + System.nanoTime() + "-" + audience.hashCode();
        AppRegistration registration = new AppRegistration(appId, "encrypt,decrypt", "ssjwt multi-aud test", true);
        registration.setSigningPublicKeyPem(pemEncodePublicKey(keyPair.getPublic()));
        appRegistrationRepository.save(registration);

        String token = signedTokenFor(appId, (RSAPrivateKey) keyPair.getPrivate(), audience);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Authorization", "Bearer " + token);
        headers.set("X-App-ID", appId);
        HttpEntity<Map<String, Object>> req = new HttpEntity<>(Map.of("plaintext", "multi-aud test"), headers);

        ResponseEntity<Map> resp = rest.postForEntity("/api/sensec/hsm/v1/encrypt", req, Map.class);

        assertEquals(HttpStatus.CREATED, resp.getStatusCode());
    }
}
