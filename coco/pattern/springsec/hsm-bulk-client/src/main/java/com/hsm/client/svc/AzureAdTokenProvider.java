package com.hsm.client.svc;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
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
 * <p>No client secret anywhere in this chain -- the credential cascade below
 * (Workload Identity first) derives everything from the pod's own Kubernetes
 * identity via a kubelet-rotated federated token file, same as
 * AzureKeyVaultKekClient.buildCredential() (hsm-bulk-service) and
 * AdlsFileStore.buildCredential() (this module) already do for their own
 * purposes -- kept as its own copy here rather than shared, matching this
 * repo's no-shared-library-between-modules convention.
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
        if (clientId != null && !clientId.isBlank()) {
            return new ManagedIdentityCredentialBuilder().clientId(clientId).build();
        }
        return new DefaultAzureCredentialBuilder().build();
    }
}
