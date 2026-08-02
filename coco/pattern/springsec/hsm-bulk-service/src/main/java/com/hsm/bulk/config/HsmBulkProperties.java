package com.hsm.bulk.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Typed config for hsm-bulk-service -- deliberately scoped down from
 * com.hsm.core.config.HsmProperties (no Splunk/PBAC/DEK-cache/rotation-cron
 * fields, none of which this PoC module needs). jwt/azure share the exact same
 * underlying env var names as hsm-core-service (see application.yml) so both
 * services authenticate the same per-app JWTs and, when demoMode=false and
 * skipAkv=false, point at the same real Key Vault.
 *
 * <p>demoMode and skipAkv are independent levers -- the exact same pair and
 * relationship as com.hsm.core.config.HsmProperties': demoMode also swaps the
 * JwtValidator (real JWTs vs fixed demo tokens, see CryptoBeansConfig), while
 * skipAkv only ever affects the KekClient -- so demoMode=false + skipAkv=true
 * gets real JWT/scope validation without requiring a reachable Key
 * Vault/Managed HSM. Named to match hsm-core-service exactly (previously
 * mockKek here), since it governs the same two things demoMode does there,
 * not just the KEK client the old name implied.
 */
@ConfigurationProperties(prefix = "hsm")
public record HsmBulkProperties(
        boolean demoMode,
        boolean skipAkv,
        Azure azure,
        Database database,
        Jwt jwt,
        Service service
) {

    public record Azure(
            String clientId,
            String tenantId,
            String keyvaultUrl,
            String kekName,
            String kekVersion,
            String keyvaultSecretUrl
    ) {
    }

    /** Schema consumer, not owner -- see hsm-bulk-service/pom.xml's comment. No Flyway, ddl-auto: none. */
    public record Database(
            String url
    ) {
    }

    public record Jwt(
            String publicKeyPem,
            String jwksUrl,
            String audience,
            String issuer
    ) {
    }

    public record Service(
            String apiV1Prefix,
            int dekBatchMaxItems
    ) {
    }
}
