package com.hsm.cekrotation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Standalone, headless worker (no HTTP listener -- spring.main.web-application-type=none)
 * that rotates the Redis DEK-cache encryption key via an alpha/beta Key Vault
 * secret-slot scheme. Ported from cek_rotation/main.py.
 */
@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class CekRotationApplication {

    // Static initializer, not @PostConstruct -- see FipsBootstrap's javadoc.
    static {
        FipsBootstrap.register();
    }

    public static void main(String[] args) {
        SpringApplication.run(CekRotationApplication.class, args);
    }
}
