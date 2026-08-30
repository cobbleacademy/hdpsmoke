package com.hsm.client.file;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
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
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
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
        // A real streaming read straight from the SDK, matching
        // AzureBlobFileStore.openRead()'s own openInputStream() -- not the
        // download-to-a-local-temp-file-then-read-that-back approach this used
        // before. That extra temp-file hop was implicated in a real,
        // reproducible corruption: FileBulkJob.decryptOneFile succeeded past
        // AES-GCM's own tag check (so the ciphertext/IV/tag it read were
        // genuinely authentic to something) but then failed to base64-decode
        // the result -- yet decrypting the exact same ADLS blob after
        // downloading it with `az storage fs file download` and pointing
        // FileBulkJob at that local copy worked. Same bytes in ADLS, different
        // outcome depending on how they were read back into this JVM -- so the
        // bug was in this method's own read path, not the blob or the crypto.
        DataLakeFileClient fileClient = fileSystemClient.getFileClient(join(rootPath, relativePath));
        return fileClient.openInputStream().getInputStream();
    }

    @Override
    public OutputStream openWrite(String relativePath) {
        DataLakeFileClient fileClient = fileSystemClient.getFileClient(join(rootPath, relativePath));
        // ADLS Gen2's upload API is pull-based (reads from an InputStream), while
        // FileBulkJob writes push-style (OutputStream) to stay symmetric with
        // LocalFileStore -- bridge with a piped stream, uploading on a background
        // thread as the caller writes.
        //
        // The returned stream's close() blocks until that upload thread actually
        // finishes and rethrows anything it threw -- closing a PipedOutputStream
        // only signals EOF to the reader, it does not wait for the upload to
        // finish committing to ADLS. Without this join, a caller (FileBulkJob)
        // that closes and moves on -- or a short-lived process that exits right
        // after, killing this daemon thread outright -- can believe the write
        // succeeded while the blob is still incomplete on the ADLS side. Found
        // live: a DECRYPT job reading back a file this store had "finished"
        // writing failed to decode a chunk as valid base64, i.e. it read past
        // where the real upload had actually gotten to.
        try {
            PipedOutputStream out = new PipedOutputStream();
            PipedInputStream in = new PipedInputStream(out, 64 * 1024);
            AtomicReference<Throwable> uploadError = new AtomicReference<>();
            Thread uploader = new Thread(() -> {
                try {
                    fileClient.create(true);
                    fileClient.upload(in, -1, true);
                } catch (Throwable t) {
                    uploadError.set(t);
                }
            });
            uploader.setDaemon(true);
            uploader.start();
            return new FilterOutputStream(out) {
                @Override
                public void close() throws IOException {
                    super.close();
                    try {
                        uploader.join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted waiting for ADLS upload to finish: " + relativePath, e);
                    }
                    Throwable err = uploadError.get();
                    if (err != null) {
                        throw new IOException("ADLS upload failed for " + relativePath, err);
                    }
                }
            };
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
