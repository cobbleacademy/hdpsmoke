package com.hsm.edekmigrate;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientCertificateCredentialBuilder;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.identity.WorkloadIdentityCredentialBuilder;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.CryptographyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.models.KeyWrapAlgorithm;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A Key Vault or Managed HSM, with the same wrap and unwrap as hsm-core's
 * {@code AzureKeyVaultKekClient}: RSA-OAEP-256; unwrap pinned to the EDEK's KEK
 * version; wrap with the versionless key id, so the HSM uses the key's latest
 * version and reports it back; the SDK's key caching off, so every operation runs
 * in the HSM.
 */
public final class AzureKekVault implements KekVault {

    static final KeyWrapAlgorithm WRAP_ALGORITHM = KeyWrapAlgorithm.RSA_OAEP_256;

    private final String vaultUrl;
    private final TokenCredential credential;
    private final ConcurrentHashMap<String, CryptographyClient> wrapClients = new ConcurrentHashMap<>();

    public AzureKekVault(String vaultUrl, TokenCredential credential) {
        this.vaultUrl = trimTrailingSlash(vaultUrl);
        this.credential = credential;
    }

    public static AzureKekVault of(MigrateConfig.Side side) {
        return new AzureKekVault(side.vaultUrl(), credential(side.credential()));
    }

    @Override
    public byte[] unwrap(byte[] edek, String kekName, String kekVersion) {
        return client(keyId(kekName) + "/" + kekVersion).unwrapKey(WRAP_ALGORITHM, edek).getKey();
    }

    @Override
    public Wrapped wrap(byte[] dek, String kekName) {
        var result = wrapClients.computeIfAbsent(kekName, n -> client(keyId(n))).wrapKey(WRAP_ALGORITHM, dek);
        return new Wrapped(result.getEncryptedKey(), versionFromKeyId(result.getKeyId()));
    }

    private String keyId(String kekName) {
        return vaultUrl + "/keys/" + kekName;
    }

    private CryptographyClient client(String keyId) {
        return new CryptographyClientBuilder().keyIdentifier(keyId).credential(credential).disableKeyCaching()
                .buildClient();
    }

    /** The last path segment of the key id the HSM returns: the version it used. */
    static String versionFromKeyId(String keyId) {
        String trimmed = trimTrailingSlash(keyId);
        String version = trimmed.substring(trimmed.lastIndexOf('/') + 1);
        if (version.isBlank() || trimmed.endsWith("/keys/" + version)) {
            throw new IllegalStateException("the HSM returned a key id without a version");
        }
        return version;
    }

    private static String trimTrailingSlash(String s) {
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }

    static TokenCredential credential(MigrateConfig.CredentialConfig c) {
        return switch (c.mode()) {
            case "client-secret" -> new ClientSecretCredentialBuilder()
                    .tenantId(c.tenantId()).clientId(c.clientId()).clientSecret(c.clientSecret()).build();
            case "client-certificate" -> new ClientCertificateCredentialBuilder()
                    .tenantId(c.tenantId()).clientId(c.clientId()).pemCertificate(c.certificatePath()).build();
            case "workload-identity" -> new WorkloadIdentityCredentialBuilder()
                    .tenantId(c.tenantId()).clientId(c.clientId()).tokenFilePath(c.tokenFile()).build();
            case "managed-identity" -> c.clientId() == null
                    ? new ManagedIdentityCredentialBuilder().build()
                    : new ManagedIdentityCredentialBuilder().clientId(c.clientId()).build();
            case "default" -> new DefaultAzureCredentialBuilder().build();
            default -> throw new IllegalArgumentException("unknown credential mode: " + c.mode());
        };
    }
}
