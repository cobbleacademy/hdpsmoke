package com.hsm.core.crypto;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.models.KeyWrapAlgorithm;
import com.azure.security.keyvault.keys.cryptography.models.WrapResult;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The wrapping client must follow Key Vault rotations without a pod restart: it
 * is built from the versionless key id with SDK key caching off, so every wrap is
 * a {@code POST /keys/<name>//wrapkey} (no version: Key Vault picks the latest) and the
 * SDK never GETs the key to pin a version and wrap locally. Uses a fake HTTP
 * transport -- no Key Vault needed.
 */
class AzureKeyVaultKekClientVersionTest {

    private static final String VAULT = "https://unit-test.vault.azure.net";
    private static final TokenCredential CREDENTIAL =
            ctx -> Mono.just(new AccessToken("test-token", OffsetDateTime.now().plusHours(1)));

    /** Answers the Key Vault auth challenge, then wrap/unwrap as if the key's latest version were {@code latest}. */
    private static final class FakeVault implements HttpClient {
        final List<String> calls = new CopyOnWriteArrayList<>();
        volatile String latest = "ver1";

        @Override
        public Mono<HttpResponse> send(HttpRequest request) {
            if (request.getHeaders().getValue(HttpHeaderName.AUTHORIZATION) == null) {
                return Mono.just(new FakeResponse(request, 401, "",
                        "Bearer authorization=\"https://login.microsoftonline.com/tenant\", resource=\"https://vault.azure.net\""));
            }
            String path = request.getUrl().getPath();
            calls.add(request.getHttpMethod() + " " + path);
            if (request.getHttpMethod() == HttpMethod.POST && path.endsWith("/wrapkey")) {
                String kid = VAULT + "/keys/kek1/" + latest;
                return Mono.just(new FakeResponse(request, 200,
                        "{\"kid\":\"" + kid + "\",\"value\":\"" + b64url("wrapped") + "\"}", null));
            }
            if (request.getHttpMethod() == HttpMethod.POST && path.endsWith("/unwrapkey")) {
                String kid = VAULT + path.substring(0, path.length() - "/unwrapkey".length());
                return Mono.just(new FakeResponse(request, 200,
                        "{\"kid\":\"" + kid + "\",\"value\":\"" + b64url("plain-dek") + "\"}", null));
            }
            return Mono.just(new FakeResponse(request, 404, "{\"error\":{\"code\":\"NotFound\"}}", null));
        }

        private static String b64url(String s) {
            return Base64.getUrlEncoder().withoutPadding().encodeToString(s.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static final class FakeResponse extends HttpResponse {
        private final int status;
        private final byte[] body;
        private final HttpHeaders headers = new HttpHeaders();

        FakeResponse(HttpRequest request, int status, String body, String wwwAuthenticate) {
            super(request);
            this.status = status;
            this.body = body.getBytes(StandardCharsets.UTF_8);
            headers.set(HttpHeaderName.CONTENT_TYPE, "application/json");
            if (wwwAuthenticate != null) {
                headers.set(HttpHeaderName.WWW_AUTHENTICATE, wwwAuthenticate);
            }
        }

        @Override public int getStatusCode() { return status; }
        @Override @Deprecated public String getHeaderValue(String name) { return headers.getValue(name); }
        @Override public HttpHeaders getHeaders() { return headers; }
        @Override public Flux<ByteBuffer> getBody() { return Flux.just(ByteBuffer.wrap(body)); }
        @Override public Mono<byte[]> getBodyAsByteArray() { return Mono.just(body); }
        @Override public Mono<String> getBodyAsString() { return Mono.just(new String(body, StandardCharsets.UTF_8)); }
        @Override public Mono<String> getBodyAsString(Charset charset) { return Mono.just(new String(body, charset)); }
    }

    @Test
    void wrapFollowsAKeyVaultRotationWithoutRebuildingTheClient() {
        FakeVault vault = new FakeVault();
        CryptographyClient client = AzureKeyVaultKekClient.buildRemoteOnlyClient(
                AzureKeyVaultKekClient.latestVersionKeyId(VAULT, "kek1"), CREDENTIAL, vault);

        WrapResult first = client.wrapKey(KeyWrapAlgorithm.RSA_OAEP_256, new byte[32]);
        vault.latest = "ver2"; // Key Vault rotation, same long-lived client
        WrapResult second = client.wrapKey(KeyWrapAlgorithm.RSA_OAEP_256, new byte[32]);

        assertEquals("ver1", AzureKeyVaultKekClient.versionFromKeyId(first.getKeyId()));
        assertEquals("ver2", AzureKeyVaultKekClient.versionFromKeyId(second.getKeyId()));
        // The SDK sends a versionless key id as an empty version segment ("kek1//wrapkey"),
        // which Key Vault resolves to the key's latest version.
        assertEquals(List.of("POST /keys/kek1//wrapkey", "POST /keys/kek1//wrapkey"), vault.calls,
                "every wrap goes to Key Vault with no version pinned; the key is never fetched for local wrapping");
    }

    @Test
    void unwrapUsesTheRecordedVersionAndNeverFetchesTheKey() {
        FakeVault vault = new FakeVault();
        CryptographyClient client = AzureKeyVaultKekClient.buildRemoteOnlyClient(
                AzureKeyVaultKekClient.versionedKeyId(VAULT, "kek1", "ver1"), CREDENTIAL, vault);

        client.unwrapKey(KeyWrapAlgorithm.RSA_OAEP_256, new byte[256]);

        assertEquals(List.of("POST /keys/kek1/ver1/unwrapkey"), vault.calls);
    }

    @Test
    void keyIdHelpers() {
        assertEquals(VAULT + "/keys/kek1", AzureKeyVaultKekClient.latestVersionKeyId(VAULT + "/", "kek1"));
        assertEquals(VAULT + "/keys/kek1/abc", AzureKeyVaultKekClient.versionedKeyId(VAULT, "kek1", "abc"));
        assertEquals("abc123", AzureKeyVaultKekClient.versionFromKeyId(VAULT + "/keys/kek1/abc123"));
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> AzureKeyVaultKekClient.versionFromKeyId(VAULT + "/keys/kek1"));
        assertTrue(e.getMessage().contains("without a version"));
    }
}
