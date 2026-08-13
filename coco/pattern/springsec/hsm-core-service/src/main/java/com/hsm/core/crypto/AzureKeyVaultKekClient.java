package com.hsm.core.crypto;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.identity.WorkloadIdentityCredentialBuilder;
import com.azure.security.keyvault.keys.KeyClient;
import com.azure.security.keyvault.keys.KeyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.CryptographyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.models.KeyWrapAlgorithm;
import com.azure.security.keyvault.keys.models.KeyVaultKey;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hsm.core.config.HsmProperties;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Azure Key Vault HSM client for KEK wrap/unwrap operations. Ported from
 * app/crypto/kek_client.py. The KEK never leaves the HSM boundary. Authentication
 * uses Managed Identity / Workload Identity federation -- no static credentials.
 */
public class AzureKeyVaultKekClient implements KekClient {

    // RSA-OAEP-SHA-256 is the FIPS-approved wrapping algorithm for RSA keys in AKV HSM.
    private static final KeyWrapAlgorithm WRAP_ALGORITHM = KeyWrapAlgorithm.RSA_OAEP_256;
    private static final String DEFAULT_TOKEN_FILE = "/var/run/secrets/azure/tokens/azure-identity-token";

    private final String kekName;
    private final String kekVersion;
    private final String keyvaultUrl;
    private final TokenCredential credential;
    private final KeyClient keyClient;
    private final SecretClient secretClient;

    private volatile CryptographyClient cryptographyClient;
    private final ReentrantLock lock = new ReentrantLock();

    public AzureKeyVaultKekClient(HsmProperties properties) {
        HsmProperties.Azure azureProps = properties.azure();
        this.kekName = azureProps.kekName();
        this.kekVersion = azureProps.kekVersion();
        this.keyvaultUrl = azureProps.keyvaultUrl();
        this.credential = buildCredential(azureProps);
        this.keyClient = new KeyClientBuilder().vaultUrl(keyvaultUrl).credential(credential).buildClient();

        // Managed HSM does not support the Secrets API. Plain secrets (Splunk HEC token,
        // DEK cache CEK) must live in a regular Key Vault at this URL, falling back to
        // the same vault URL (valid only when that URL is itself a regular vault).
        String secretVaultUrl = azureProps.keyvaultSecretUrl().isBlank() ? keyvaultUrl : azureProps.keyvaultSecretUrl();
        this.secretClient = new SecretClientBuilder().vaultUrl(secretVaultUrl).credential(credential).buildClient();
    }

    private CryptographyClient getCryptographyClient() {
        CryptographyClient client = cryptographyClient;
        if (client == null) {
            lock.lock();
            try {
                client = cryptographyClient;
                if (client == null) {
                    KeyVaultKey key = kekVersion.isBlank() ? keyClient.getKey(kekName) : keyClient.getKey(kekName, kekVersion);
                    client = new CryptographyClientBuilder().keyIdentifier(key.getId()).credential(credential).buildClient();
                    cryptographyClient = client;
                }
            } finally {
                lock.unlock();
            }
        }
        return client;
    }

    @Override
    public WrapResult wrapDek(byte[] dek) {
        var result = getCryptographyClient().wrapKey(WRAP_ALGORITHM, dek);
        String keyId = result.getKeyId();
        String version = keyId.substring(keyId.lastIndexOf('/') + 1);
        return new WrapResult(result.getEncryptedKey(), version);
    }

    @Override
    public byte[] unwrapDek(byte[] edek, String kekVersionToUse) {
        // Unwrap with the *specific* KEK version this EDEK was wrapped with -- old
        // versions remain usable after rotation until all EDEKs are re-wrapped.
        String keyId = trimTrailingSlash(keyvaultUrl) + "/keys/" + kekName + "/" + kekVersionToUse;
        CryptographyClient versioned = new CryptographyClientBuilder().keyIdentifier(keyId).credential(credential).buildClient();
        var result = versioned.unwrapKey(WRAP_ALGORITHM, edek);
        return result.getKey();
    }

    @Override
    public String getCurrentKekVersion() {
        KeyVaultKey key = keyClient.getKey(kekName);
        String version = key.getProperties().getVersion();
        return version == null ? "" : version;
    }

    @Override
    public String fetchSecret(String secretName) {
        KeyVaultSecret secret = secretClient.getSecret(secretName);
        return secret.getValue() == null ? "" : secret.getValue();
    }

    @Override
    public SecretWithVersion fetchSecretWithVersion(String secretName) {
        KeyVaultSecret secret = secretClient.getSecret(secretName);
        String value = secret.getValue() == null ? "" : secret.getValue();
        String id = secret.getProperties().getId();
        String kvVersion = id == null ? "" : lastPathSegment(id);
        return new SecretWithVersion(value, kvVersion);
    }

    @Override
    public void close() {
        // Azure SDK sync clients hold no persistent connection/resources requiring explicit close.
    }

    private static String lastPathSegment(String uri) {
        String trimmed = trimTrailingSlash(uri);
        return trimmed.substring(trimmed.lastIndexOf('/') + 1);
    }

    private static String trimTrailingSlash(String s) {
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }

    /**
     * Build the Azure credential without requiring env var injection.
     *
     * Resolution order for each parameter:
     *   client_id  : config -&gt; AZURE_CLIENT_ID env -&gt; JWT appid/azp claim
     *   tenant_id  : config -&gt; AZURE_TENANT_ID env -&gt; JWT tid claim
     *   token_file : AZURE_FEDERATED_TOKEN_FILE env -&gt; well-known path
     *
     * If all three resolve, uses WorkloadIdentityCredential directly. Falls back to
     * DefaultAzureCredential only when the token file is absent (local dev / demo mode).
     */
    private static TokenCredential buildCredential(HsmProperties.Azure azureProps) {
        String tokenFile = System.getenv().getOrDefault("AZURE_FEDERATED_TOKEN_FILE", DEFAULT_TOKEN_FILE);
        JsonNode claims = decodeTokenClaims(tokenFile);

        String clientId = firstNonBlank(
                azureProps.clientId(),
                System.getenv("AZURE_CLIENT_ID"),
                textOrNull(claims, "appid"),
                textOrNull(claims, "azp")
        );
        String tenantId = firstNonBlank(
                azureProps.tenantId(),
                System.getenv("AZURE_TENANT_ID"),
                textOrNull(claims, "tid")
        );

        if (clientId != null && tenantId != null && new File(tokenFile).exists()) {
            return new WorkloadIdentityCredentialBuilder()
                    .tenantId(tenantId)
                    .clientId(clientId)
                    .tokenFilePath(tokenFile)
                    .build();
        }

        // Explicit App Registration secret, only when configured -- checked before the
        // Managed Identity/IMDS fallback below since a caller who set a client-secret
        // clearly intends to use it, not fall through to node-identity resolution.
        // Requires clientId AND tenantId to also be set (ClientSecretCredential needs
        // both, unlike Workload Identity above which can derive them from the
        // federated token's own claims).
        if (!azureProps.clientSecret().isBlank() && clientId != null && tenantId != null) {
            return new ClientSecretCredentialBuilder()
                    .tenantId(tenantId)
                    .clientId(clientId)
                    .clientSecret(azureProps.clientSecret())
                    .build();
        }

        // No federated credential or explicit secret configured -- fall back to node
        // managed identity via IMDS. Requires the AKS node pool to have a user-assigned
        // managed identity with Key Vault permissions attached at the infrastructure
        // level -- a materially different setup than Workload Identity above, and NOT
        // what a client-id meant for Workload Identity federation will satisfy.
        if (!azureProps.clientId().isBlank()) {
            return new ManagedIdentityCredentialBuilder().clientId(azureProps.clientId()).build();
        }

        return new DefaultAzureCredentialBuilder().build();
    }

    /** Decode the injected JWT payload without verifying signature. Returns null on any error. */
    private static JsonNode decodeTokenClaims(String tokenFile) {
        try {
            File file = new File(tokenFile);
            if (!file.exists()) {
                return null;
            }
            String token = Files.readString(file.toPath(), StandardCharsets.UTF_8).trim();
            String[] parts = token.split("\\.");
            if (parts.length < 2) {
                return null;
            }
            String payload = parts[1];
            payload = payload + "=".repeat((4 - payload.length() % 4) % 4);
            byte[] decoded = Base64.getUrlDecoder().decode(payload);
            return new ObjectMapper().readTree(decoded);
        } catch (IOException | IllegalArgumentException e) {
            return null;
        }
    }

    private static String textOrNull(JsonNode node, String field) {
        if (node == null || !node.has(field)) {
            return null;
        }
        return node.get(field).asText(null);
    }

    private static String firstNonBlank(String... values) {
        for (String v : values) {
            if (v != null && !v.isBlank()) {
                return v;
            }
        }
        return null;
    }
}
