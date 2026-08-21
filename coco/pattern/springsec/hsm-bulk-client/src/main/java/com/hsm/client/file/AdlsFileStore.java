package com.hsm.client.file;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.identity.WorkloadIdentityCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.ListPathsOptions;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * ADLS Gen2 FileStore. Root is a directory URI of the form
 * {@code abfss://<container>@<account>.dfs.core.windows.net/<path>} (the standard
 * ADLS Gen2 URI form) -- <path> may be empty (root of the container).
 *
 * <p>Credential resolution mirrors AzureKeyVaultKekClient's fallback chain
 * (WorkloadIdentityCredential -> ManagedIdentityCredential -> DefaultAzureCredential)
 * -- not a new auth mechanism, the same pattern applied to a different Azure SDK
 * client. Unlike AzureKeyVaultKekClient, this has no HsmBulkProperties.Azure-shaped
 * config to read client-id/tenant-id from (this module has no such config section),
 * so it resolves purely from the environment: AZURE_CLIENT_ID/AZURE_TENANT_ID env
 * vars and the well-known federated token file path -- the same values a workload
 * identity webhook injects automatically in AKS, requiring no additional config here.
 *
 * <p>accountKey (from config, see ClientProperties.File.StoreRef's javadoc) is a
 * deliberate escape hatch, not a second normal auth mode: when set, this class uses
 * StorageSharedKeyCredential instead of the chain above, entirely bypassing it. This
 * exists only so encrypt/decrypt can be validated against a real ADLS container
 * before the deployment identity's RBAC data-plane role is actually granted -- it
 * must not be set in the real deployment's config.
 */
public class AdlsFileStore implements FileStore {

    private static final String DEFAULT_TOKEN_FILE = "/var/run/secrets/azure/tokens/azure-identity-token";

    private final DataLakeFileSystemClient fileSystemClient;
    private final String rootPath; // may be "" for container root

    public AdlsFileStore(String rootUri, String accountKey) {
        Parsed parsed = parse(rootUri);
        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder()
                .endpoint("https://" + parsed.accountHost());
        if (accountKey != null && !accountKey.isBlank()) {
            builder.credential(new StorageSharedKeyCredential(accountName(parsed.accountHost()), accountKey));
        } else {
            builder.credential(buildCredential());
        }
        DataLakeServiceClient serviceClient = builder.buildClient();
        this.fileSystemClient = serviceClient.getFileSystemClient(parsed.container());
        this.rootPath = parsed.path();
    }

    @Override
    public List<String> list(List<String> fileTypes) {
        DataLakeDirectoryClient rootDir = fileSystemClient.getDirectoryClient(rootPath);
        ListPathsOptions options = new ListPathsOptions().setRecursive(true).setPath(rootPath.isEmpty() ? null : rootPath);
        return fileSystemClient.listPaths(options, null).stream()
                .filter(item -> !item.isDirectory())
                .filter(item -> matchesType(item, fileTypes))
                .map(PathItem::getName)
                .map(name -> rootPath.isEmpty() ? name : name.substring(rootPath.length() + 1))
                .filter(relativePath -> !relativePath.startsWith(MANIFEST_DIR + "/"))
                .collect(Collectors.toList());
    }

    @Override
    public InputStream openRead(String relativePath) {
        DataLakeFileClient fileClient = fileSystemClient.getFileClient(join(rootPath, relativePath));
        try {
            File tmp = File.createTempFile("adls-read-", ".tmp");
            tmp.deleteOnExit();
            fileClient.readToFile(tmp.getAbsolutePath(), true);
            return Files.newInputStream(tmp.toPath());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read ADLS path " + relativePath, e);
        }
    }

    @Override
    public OutputStream openWrite(String relativePath) {
        DataLakeFileClient fileClient = fileSystemClient.getFileClient(join(rootPath, relativePath));
        // ADLS Gen2's upload API is pull-based (reads from an InputStream), while
        // FileBulkJob writes push-style (OutputStream) to stay symmetric with
        // LocalFileStore -- bridge with a piped stream, uploading on a background
        // thread as the caller writes.
        try {
            PipedOutputStream out = new PipedOutputStream();
            PipedInputStream in = new PipedInputStream(out, 64 * 1024);
            Thread uploader = new Thread(() -> {
                fileClient.create(true);
                fileClient.upload(in, -1, true);
            });
            uploader.setDaemon(true);
            uploader.start();
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open ADLS path " + relativePath + " for write", e);
        }
    }

    private static boolean matchesType(PathItem item, List<String> fileTypes) {
        if (fileTypes == null || fileTypes.isEmpty()) {
            return true;
        }
        String name = item.getName().toLowerCase(Locale.ROOT);
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

    private record Parsed(String container, String accountHost, String path) {
    }

    private static Parsed parse(String uri) {
        String withoutScheme = uri.replaceFirst("^abfss://", "");
        int at = withoutScheme.indexOf('@');
        if (at < 0) {
            throw new IllegalArgumentException("ADLS root must be abfss://<container>@<account>.dfs.core.windows.net/<path>: " + uri);
        }
        String container = withoutScheme.substring(0, at);
        String rest = withoutScheme.substring(at + 1);
        int slash = rest.indexOf('/');
        String accountHost = slash < 0 ? rest : rest.substring(0, slash);
        String path = slash < 0 ? "" : rest.substring(slash + 1).replaceAll("/+$", "");
        return new Parsed(container, accountHost, path);
    }

    /** accountHost is "<account>.dfs.core.windows.net" -- StorageSharedKeyCredential needs just the account name. */
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
        if (clientId != null && !clientId.isBlank()) {
            return new ManagedIdentityCredentialBuilder().clientId(clientId).build();
        }
        return new DefaultAzureCredentialBuilder().build();
    }
}
