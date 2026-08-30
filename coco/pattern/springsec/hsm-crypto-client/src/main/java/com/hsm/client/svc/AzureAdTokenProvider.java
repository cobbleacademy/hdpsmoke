package com.hsm.client.svc;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.identity.WorkloadIdentityCredentialBuilder;

import java.io.File;

/**
 * Acquires a real Azure AD access token fresh before every call, instead of a
 * static config value that would expire on any job running longer than the
 * token's TTL (~1h). TokenCredential.getToken() already caches internally and
 * only performs the real federated-token exchange when the cached token is
 * expired or near-expiry -- calling it before every request costs nothing
 * extra on the common (cache-hit) path, and requires no manual TTL tracking
 * here at all.
 *
 * <p>No client secret required for the common (in-cluster) case -- the
 * credential cascade below tries Workload Identity first, deriving
 * everything from the pod's own Kubernetes identity via a kubelet-rotated
 * federated token file. An explicit AZURE_CLIENT_SECRET is supported as a
 * fallback (checked before Managed Identity) for callers running off
 * Azure-hosted compute entirely -- e.g. a local/manual verification run --
 * where neither Workload Identity nor Managed Identity's IMDS endpoint is
 * reachable. Same cascade shape as AzureKeyVaultKekClient.buildCredential()
 * (hsm-core-service), AdlsFileStore.buildCredential() and
 * AzureBlobFileStore.buildCredential() (hsm-bulk-client) -- kept as its own
 * copy here rather than shared, matching this repo's
 * no-shared-library-between-modules convention.
 */
public class AzureAdTokenProvider implements TokenProvider {

    private static final String DEFAULT_TOKEN_FILE = "/var/run/secrets/azure/tokens/azure-identity-token";

    private final TokenCredential credential;
    private final TokenRequestContext context;

    public AzureAdTokenProvider(String scope) {
        this.credential = buildCredential();
        this.context = new TokenRequestContext().addScopes(scope);
    }

    @Override
    public String getBearerToken() {
        return credential.getToken(context).block().getToken();
    }

    private static TokenCredential buildCredential() {
        String tokenFile = System.getenv().getOrDefault("AZURE_FEDERATED_TOKEN_FILE", DEFAULT_TOKEN_FILE);
        String clientId = System.getenv("AZURE_CLIENT_ID");
        String tenantId = System.getenv("AZURE_TENANT_ID");

        if (clientId != null && !clientId.isBlank() && tenantId != null && !tenantId.isBlank() && new File(tokenFile).exists()) {
            return new WorkloadIdentityCredentialBuilder()
                    .tenantId(tenantId)
                    .clientId(clientId)
                    .tokenFilePath(tokenFile)
                    .build();
        }
        // Explicit App Registration secret, only when configured -- checked before the
        // Managed Identity/IMDS fallback below since a caller who set a client secret
        // clearly intends to use it, not fall through to node-identity resolution (which
        // also fails outright off Azure-hosted compute, e.g. a local dev machine). Same
        // ordering as AzureKeyVaultKekClient.buildCredential() (hsm-core-service).
        String clientSecret = System.getenv("AZURE_CLIENT_SECRET");
        if (clientSecret != null && !clientSecret.isBlank() && clientId != null && !clientId.isBlank()
                && tenantId != null && !tenantId.isBlank()) {
            return new ClientSecretCredentialBuilder()
                    .tenantId(tenantId)
                    .clientId(clientId)
                    .clientSecret(clientSecret)
                    .build();
        }
        if (clientId != null && !clientId.isBlank()) {
            return new ManagedIdentityCredentialBuilder().clientId(clientId).build();
        }
        return new DefaultAzureCredentialBuilder().build();
    }
}
