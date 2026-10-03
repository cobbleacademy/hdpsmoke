package com.hsm.fileservice;

import com.hsm.client.config.FipsBootstrap;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * hsm-file-service: the continuous counterpart of hsm-bulk-client. Runs in a
 * consumer's namespace, serves decrypted files to that consumer's BFF over
 * {@code GET <api-prefix>/files/{path}} (default prefix /api/sensec/file/v1), and nothing else. See java/docs/FILE_SERVICE.md.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class FileServiceApplication {

    static {
        // Before any crypto class loads: DekManager/IvFactory capture the FIPS DRBG statically.
        FipsBootstrap.register();
    }

    public static void main(String[] args) {
        SpringApplication.run(FileServiceApplication.class, args);
    }
}
