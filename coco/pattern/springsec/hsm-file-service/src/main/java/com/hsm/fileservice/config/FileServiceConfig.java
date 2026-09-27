package com.hsm.fileservice.config;

import com.hsm.client.HsmCryptoClient;
import com.hsm.client.fileformat.EncryptedFileReader;
import com.hsm.filestore.AdlsFileStore;
import com.hsm.filestore.AzureBlobFileStore;
import com.hsm.filestore.FileStore;
import com.hsm.filestore.LocalFileStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Semaphore;

@Configuration
public class FileServiceConfig {

    private static final Logger log = LoggerFactory.getLogger(FileServiceConfig.class);

    /**
     * One client for the process: it owns the DEK cache (TTL = revocation lag) and the
     * connection to hsm-core-service. Closed on shutdown, which zeroes every cached DEK.
     */
    @Bean(destroyMethod = "close")
    public HsmCryptoClient hsmCryptoClient(FileServiceProperties props) {
        FileServiceProperties.Core core = props.core();
        require(core.baseUrl(), "hsm.file-service.core.base-url");
        require(core.appId(), "hsm.file-service.core.app-id");
        HsmCryptoClient.Builder b = HsmCryptoClient.builder()
                .baseUrl(core.baseUrl())
                .apiV1Prefix(core.apiV1Prefix())
                .appId(core.appId())
                .privateKeyPem(readPem(core.privateKeyPemFile(), "hsm.file-service.core.private-key-pem-file"))
                .dekCacheTtl(props.dekCache().ttl())
                .dekCacheMaxSize(props.dekCache().maxSize());
        switch (core.authMode()) {
            case STATIC -> b.staticToken(require(core.staticToken(), "hsm.file-service.core.static-token"));
            case AZURE_AD -> b.azureAdToken(require(core.azureTokenScope(), "hsm.file-service.core.azure-token-scope"));
            case SELF_SIGNED_JWT -> b.selfSignedJwt(
                    readPem(core.signingKeyPemFile(), "hsm.file-service.core.signing-key-pem-file"), core.jwtAudience());
            case MTLS -> b.mtls(
                    readPem(core.mtlsCertPemFile(), "hsm.file-service.core.mtls-cert-pem-file"),
                    readPem(core.mtlsKeyPemFile(), "hsm.file-service.core.mtls-key-pem-file"));
        }
        log.info("file_service_core_client app_id={} auth_mode={} dek_cache_ttl={} dek_cache_max={}",
                core.appId(), core.authMode(), props.dekCache().ttl(), props.dekCache().maxSize());
        return b.build();
    }

    @Bean
    public FileStore fileStore(FileServiceProperties props) {
        FileServiceProperties.Store s = props.store();
        if (s == null || s.type() == null) {
            throw new IllegalStateException("hsm.file-service.store.type is required (LOCAL, ADLS or AZURE_BLOB)");
        }
        require(s.root(), "hsm.file-service.store.root");
        if (s.accountKey() != null && !s.accountKey().isBlank()) {
            log.warn("file_service_store_shared_key_auth -- store.account-key is set; this bypasses Workload Identity and is for pre-RBAC validation only");
        }
        return switch (s.type()) {
            case LOCAL -> new LocalFileStore(s.root());
            case ADLS -> new AdlsFileStore(s.root(), s.accountKey());
            case AZURE_BLOB -> new AzureBlobFileStore(s.root(), s.accountKey());
        };
    }

    @Bean
    public EncryptedFileReader.Limits readerLimits(FileServiceProperties props) {
        FileServiceProperties.Limits l = props.limits();
        return new EncryptedFileReader.Limits(l.maxFrameBytes(), l.maxChunkPlaintextBytes());
    }

    /** Buffer slots for the verify-before-send path; bounded so small-file bursts can't exhaust the heap. */
    @Bean
    public Semaphore bufferSlots(FileServiceProperties props) {
        return new Semaphore(props.delivery().maxBufferedRequests());
    }

    private static String require(String value, String property) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(property + " is required");
        }
        return value;
    }

    private static String readPem(String file, String property) {
        require(file, property);
        try {
            return Files.readString(Path.of(file));
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + property + " (" + file + ")", e);
        }
    }
}
