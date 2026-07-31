package com.hsm.bulk.crypto;

import com.azure.core.credential.TokenCredential;
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
import com.hsm.bulk.config.HsmBulkProperties;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Duplicated from com.hsm.core.crypto.AzureKeyVaultKekClient -- same KEK wrap/unwrap
 * logic, only the config type it's constructed from changed
 * (HsmBulkProperties.Azure instead of HsmProperties.Azure, identical shape).
 */
public class AzureKeyVaultKekClient implements KekClient {

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

    public AzureKeyVaultKekClient(HsmBulkProperties properties) {
        HsmBulkProperties.Azure azureProps = properties.azure();
        this.kekName = azureProps.kekName();
        this.kekVersion = azureProps.kekVersion();
        this.keyvaultUrl = azureProps.keyvaultUrl();
        this.credential = buildCredential(azureProps);
        this.keyClient = new KeyClientBuilder().vaultUrl(keyvaultUrl).credential(credential).buildClient();

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

    private static TokenCredential buildCredential(HsmBulkProperties.Azure azureProps) {
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

        if (!azureProps.clientId().isBlank()) {
            return new ManagedIdentityCredentialBuilder().clientId(azureProps.clientId()).build();
        }

        return new DefaultAzureCredentialBuilder().build();
    }

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
