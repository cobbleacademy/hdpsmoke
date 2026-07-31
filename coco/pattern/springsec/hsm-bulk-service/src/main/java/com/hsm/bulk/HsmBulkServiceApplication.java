package com.hsm.bulk;

import com.hsm.bulk.config.FipsBootstrap;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.security.autoconfigure.UserDetailsServiceAutoConfiguration;

/**
 * UserDetailsServiceAutoConfiguration excluded for the same reason as
 * hsm-core-service's own HsmCoreServiceApplication -- this API has no concept of
 * a Spring Security "user"; JwtAppIdAuthenticationFilter builds Authentication
 * directly from JWT+X-App-ID+DB scopes.
 */
@SpringBootApplication(exclude = {
        UserDetailsServiceAutoConfiguration.class
})
@ConfigurationPropertiesScan
public class HsmBulkServiceApplication {

    static {
        FipsBootstrap.register();
    }

    public static void main(String[] args) {
        SpringApplication.run(HsmBulkServiceApplication.class, args);
    }
}
