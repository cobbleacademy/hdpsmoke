package com.hsm.fileservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * hsm-file-service: the continuous counterpart of hsm-bulk-client. Runs in a
 * consumer's namespace, serves decrypted files to that consumer's BFF over
 * {@code GET <api-prefix>/files/{path}} (default prefix /api/sensec/file/v1), and nothing else. See java/docs/FILE_SERVICE.md.
 *
 * <p>Built on the chassis: hsm-chassis-core registers BC-FIPS before any bean exists
 * (and refuses to start outside approved-only mode); hsm-chassis-web provides the
 * X-Request-Id filter and the {@code <api-prefix>/docs} redirect.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class FileServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(FileServiceApplication.class, args);
    }
}
