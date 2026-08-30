package com.hsm.client.file;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.identity.WorkloadIdentityCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Plain Azure Blob Storage FileStore -- no Hierarchical Namespace required,
 * and (unlike {@link AdlsFileStore}'s Data Lake Gen2 REST API) no known
 * conflict with the account-level "soft delete for blobs" feature. That
 * combination -- HNS disabled (or in some configurations, even enabled) plus
 * blob soft delete -- is a real, Microsoft-documented account-level
 * incompatibility with certain Data Lake Gen2 operations
 * ({@code EndpointUnsupportedAccountFeatures}), not something either SDK or
 * this module can work around; this store exists as the alternative for
 * accounts where that applies.
 *
 * <p>Root is a full blob-endpoint URL:
 * {@code https://<account>.blob.core.windows.net/<container>/<path>}
 * ({@code <path>} may be empty for the container root) -- deliberately not
 * reusing {@link AdlsFileStore}'s {@code abfss://} scheme, since that scheme
 * specifically means "Data Lake Gen2 API," and mixing the two here would be
 * misleading about which REST surface a given root actually talks to.
 *
 * <p>Blob Storage has no real directory hierarchy -- relativePath is simply
 * prepended with rootPath and used as the blob name directly ("/" in a blob
 * name is a naming convention, not a real filesystem operation), which is
 * functionally equivalent to {@link LocalFileStore}/{@link AdlsFileStore}'s
 * directory semantics for everything {@code FileBulkJob} actually does
 * (list under a prefix, read/write one blob at a time -- no renames, no
 * ACLs, no true directory objects needed).
 *
 * <p>Credential resolution mirrors {@link AdlsFileStore}'s own fallback
 * chain (WorkloadIdentityCredential -&gt; ManagedIdentityCredential -&gt;
 * DefaultAzureCredential) -- duplicated here rather than shared, matching
 * this project's no-shared-library convention (see {@code AdlsFileStore}'s
 * own javadoc for the same note).
 *
 * <p>accountKey (from config, see ClientProperties.File.StoreRef's javadoc) is the
 * same deliberate escape hatch as AdlsFileStore's: when set, StorageSharedKeyCredential
 * is used instead of the chain above, entirely bypassing it, purely to validate
 * encrypt/decrypt before the deployment identity's RBAC data-plane role is granted --
 * must not be set in the real deployment's config.
 */
public class AzureBlobFileStore implements FileStore {

    private static final String DEFAULT_TOKEN_FILE = "/var/run/secrets/azure/tokens/azure-identity-token";

    private final BlobContainerClient containerClient;
    private final String rootPath; // may be "" for container root

    public AzureBlobFileStore(String rootUrl, String accountKey) {
        Parsed parsed = parse(rootUrl);
        BlobContainerClientBuilder builder = new BlobContainerClientBuilder()
                .endpoint("https://" + parsed.accountHost())
                .containerName(parsed.container());
        if (accountKey != null && !accountKey.isBlank()) {
            builder.credential(new StorageSharedKeyCredential(accountName(parsed.accountHost()), accountKey));
        } else {
            builder.credential(buildCredential());
        }
        this.containerClient = builder.buildClient();
        this.rootPath = parsed.path();
    }

    @Override
    public List<String> list(List<String> fileTypes) {
        ListBlobsOptions options = new ListBlobsOptions();
        if (!rootPath.isEmpty()) {
            options.setPrefix(rootPath + "/");
        }
        List<String> result = new ArrayList<>();
        for (BlobItem item : containerClient.listBlobs(options, null)) {
            if (Boolean.TRUE.equals(item.isPrefix())) {
                continue;
            }
            String name = item.getName();
            String relativePath = rootPath.isEmpty() ? name : name.substring(rootPath.length() + 1);
            if (relativePath.startsWith(MANIFEST_DIR + "/")) {
                continue;
            }
            if (matchesType(relativePath, fileTypes)) {
                result.add(relativePath);
            }
        }
        return result;
    }

    @Override
    public InputStream openRead(String relativePath) {
        return containerClient.getBlobClient(join(rootPath, relativePath)).openInputStream();
    }

    @Override
    public OutputStream openWrite(String relativePath) {
        BlobClient blobClient = containerClient.getBlobClient(join(rootPath, relativePath));
        BlockBlobClient blockBlobClient = blobClient.getBlockBlobClient();
        // A real streaming OutputStream from the SDK itself, unlike
        // AdlsFileStore's PipedOutputStream+background-thread bridge (the
        // Data Lake Gen2 client has no equivalent) -- overwrite=true matches
        // AdlsFileStore's fileClient.create(true) semantics.
        return blockBlobClient.getBlobOutputStream(true);
    }

    private static boolean matchesType(String relativePath, List<String> fileTypes) {
        if (fileTypes == null || fileTypes.isEmpty()) {
            return true;
        }
        String name = relativePath.toLowerCase(Locale.ROOT);
        for (String type : fileTypes) {
            if (name.endsWith(type.toLowerCase(Locale.ROOT))) {
                return true;
            }
        }
        return false;
    }

    private static String join(String rootPath, String relativePath) {
        return rootPath.isEmpty() ? relativePath : rootPath + "/" + relativePath;
    }

    record Parsed(String container, String accountHost, String path) {
    }

    // Package-private (not private) specifically so AzureBlobFileStoreTest can
    // exercise it directly -- pure parsing logic, the one part of this class
    // testable without a real Azure account/credentials.
    static Parsed parse(String url) {
        String withoutScheme = url.replaceFirst("^https://", "");
        int firstSlash = withoutScheme.indexOf('/');
        if (firstSlash < 0) {
            throw new IllegalArgumentException(
                    "Azure Blob root must be https://<account>.blob.core.windows.net/<container>/<path>: " + url);
        }
        String accountHost = withoutScheme.substring(0, firstSlash);
        String rest = withoutScheme.substring(firstSlash + 1);
        int secondSlash = rest.indexOf('/');
        String container = secondSlash < 0 ? rest : rest.substring(0, secondSlash);
        String path = secondSlash < 0 ? "" : rest.substring(secondSlash + 1).replaceAll("/+$", "");
        if (container.isEmpty()) {
            throw new IllegalArgumentException(
                    "Azure Blob root must be https://<account>.blob.core.windows.net/<container>/<path>: " + url);
        }
        return new Parsed(container, accountHost, path);
    }

    /** accountHost is "<account>.blob.core.windows.net" -- StorageSharedKeyCredential needs just the account name. */
    private static String accountName(String accountHost) {
        int dot = accountHost.indexOf('.');
        return dot < 0 ? accountHost : accountHost.substring(0, dot);
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
