package com.hsm.core;

import com.hsm.core.config.FipsBootstrap;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Redis autoconfiguration is excluded because REDIS_URL is optional infrastructure
 * (matches Python: {@code if settings.dek_cache_enabled and settings.redis_url}) --
 * DekCacheConfig builds a RedisConnectionFactory/RedisDekCache bean by hand only
 * when both dek-cache.enabled and redis.url are actually set.
 *
 * <p>UserDetailsServiceAutoConfiguration is excluded because this API has no
 * concept of a Spring Security "user" at all -- JwtAppIdAuthenticationFilter
 * builds Authentication objects directly from JWT+X-App-ID+DB scopes, never via
 * a UserDetailsService/AuthenticationManager. Without this exclusion, Spring
 * Boot creates an unused default in-memory user with a randomly generated
 * password and logs it at startup, which is just noise here.
 */
@SpringBootApplication(exclude = {
        RedisAutoConfiguration.class,
        RedisReactiveAutoConfiguration.class,
        RedisRepositoriesAutoConfiguration.class,
        UserDetailsServiceAutoConfiguration.class
})
@EnableScheduling
@ConfigurationPropertiesScan
public class HsmCoreServiceApplication {

    // A static initializer, not a @PostConstruct bean hook: Spring gives no ordering
    // guarantee between unrelated beans' @PostConstruct methods, and CryptoBeansConfig's
    // eager KekClient/DekCache beans need BC-FIPS registered before they construct. A
    // static initializer on the @SpringBootConfiguration root class runs at class-load
    // time -- before SpringApplication.run() can even begin -- for both `java -jar`
    // launches and @SpringBootTest (which loads this class directly, bypassing main()).
    static {
        FipsBootstrap.register();
    }

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(HsmCoreServiceApplication.class);
        // Auto-activates the "demo" Spring profile whenever DEMO_MODE=true is set, so
        // operators keep the same single-env-var toggle the Python service used
        // (.env.demo) instead of also having to pass --spring.profiles.active=demo.
        // Set directly on the SpringApplication instance (rather than via an
        // EnvironmentPostProcessor) so it applies deterministically before Spring
        // Boot's config-data loading decides which profile-specific YAML to load --
        // an EnvironmentPostProcessor's ordering relative to that step is unreliable
        // inside a repackaged executable jar.
        if (Boolean.parseBoolean(System.getenv("DEMO_MODE"))) {
            app.setAdditionalProfiles("demo");
        }
        app.run(args);
    }
}
