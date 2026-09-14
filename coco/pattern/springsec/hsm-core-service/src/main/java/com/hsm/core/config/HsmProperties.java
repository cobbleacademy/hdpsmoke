package com.hsm.core.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Typed binding for the service's configuration, mirroring app/config.py's
 * {@code Settings}. The underlying env var names are preserved exactly (see
 * application.yml's {@code ${ENV_VAR:default}} placeholders feeding the
 * {@code hsm.*} properties below) so existing Helm charts / .env files keep
 * working unmodified.
 */
@ConfigurationProperties(prefix = "hsm")
public record HsmProperties(
        boolean demoMode,
        boolean skipAkv,
        Azure azure,
        Database database,
        Jwt jwt,
        Service service,
        Splunk splunk,
        Pbac pbac,
        KekRotation kekRotation,
        NamedDekRotation namedDekRotation,
        Redis redis,
        DekCache dekCache,
        ClassificationGovernance classificationGovernance,
        DekNameReservation dekNameReservation
) {

    public record Azure(
            String clientId,
            String tenantId,
            // Optional. Unset (default) -- credential resolution stays federation-only
            // (Workload Identity / Managed Identity), never touching a static secret.
            // Set -- explicit App Registration client-secret auth, tried before the
            // Managed Identity/IMDS fallback (see AzureKeyVaultKekClient.buildCredential).
            // Credential-shaped: must come from a Kubernetes Secret, never a ConfigMap.
            String clientSecret,
            String keyvaultUrl,
            String kekName,
            String kekVersion,
            String keyvaultSecretUrl
    ) {
    }

    /**
     * cryptoSchema/accessSchema default to "public". JPA entities intentionally
     * carry no explicit @Table(schema=...) -- they resolve against the JDBC
     * connection's default schema, which is "public" for both H2 (demo) and a
     * stock Postgres role. If these are customized to a non-public schema name
     * in production, configure the Postgres role's default search_path (or
     * spring.datasource.hikari.connection-init-sql) to match; Flyway itself
     * always creates the tables inside the configured schema regardless.
     */
    public record Database(
            String url,
            String demoUrl,
            boolean sslEnabled,
            String sslCaCert,
            String cryptoSchema,
            String accessSchema
    ) {
    }

    /**
     * audience/issuer each accept a comma-separated list of exact values, not just
     * one -- a single Azure AD app registration legitimately produces tokens with
     * different aud/iss depending on which credential path acquired them (v1.0
     * endpoint: aud=client-id GUID, iss=https://sts.windows.net/{tenant}/; v2.0
     * endpoint: aud=App ID URI, iss=https://login.microsoftonline.com/{tenant}/v2.0).
     * RsaJwtValidator accepts a token whose aud/iss matches ANY one of the listed
     * values -- still an explicit allow-list, not a wildcard/pattern match.
     */
    public record Jwt(
            String publicKeyPem,
            String jwksUrl,
            String audience,
            String issuer
    ) {
    }

    public record Service(
            String env,
            String logLevel,
            String apiV1Prefix,
            int batchMaxItems,
            int batchExecutorPoolSize,
            int dekBatchMaxItems
    ) {
    }

    public record Splunk(
            boolean enabled,
            String hecUrl,
            String hecToken,
            String index,
            String source,
            String sourcetype,
            boolean verifySsl,
            int batchSize,
            int flushIntervalSeconds
    ) {
    }

    public record Pbac(
            boolean enabled,
            String plainidUrl,
            String plainidApiKeySecretName,
            int cacheTtlSeconds,
            boolean failOpen,
            double httpTimeoutSeconds,
            String integrationConfigPath
    ) {
    }

    public record KekRotation(
            String cron,
            boolean enabled
    ) {
    }

    /**
     * Time/policy-driven, not usage-count-driven -- SVC/hsm-core-service has no
     * visibility into how many individual values a caller actually encrypts with a
     * DEK it was handed (that happens client-side), so a lookup counter could never
     * be a trustworthy usage measure. Bounding the *age* a named DEK stays current
     * sidesteps that gap entirely instead of trying to solve it.
     */
    public record NamedDekRotation(
            String cron,
            boolean enabled,
            int maxAgeHours
    ) {
    }

    public record Redis(
            String url
    ) {
    }

    public record DekCache(
            boolean enabled,
            int ttlSeconds,
            String cekCurrentKeySecretName,
            String cekAlphaSecretName,
            String cekBetaSecretName,
            String excludedClassifications,
            int reloadIntervalSeconds
    ) {
    }

    /**
     * Phase 1 (enforce=false, default)/Phase 2 (enforce=true) toggle for
     * ClassificationGovernanceService -- see V15's migration comment and
     * that class's own javadoc. Defaults to false deliberately: flipping to
     * true the moment app_classification_grants exists would reject every
     * app that hasn't been backfilled into it yet, the same "would break
     * every existing app" concern V11's kek_registry javadoc already
     * documents for an analogous rollout. Flip only after shadow-mode
     * logging has been used to backfill real usage.
     */
    public record ClassificationGovernance(
            boolean enforce
    ) {
    }

    /**
     * Phase 1 (enforce=false, default)/Phase 2 (enforce=true) toggle for
     * DekNameReservationService -- see that class's own javadoc. An exact-
     * dek_name (tier 1) kek_registry row now carries dek_name-reservation
     * intent, not just a KEK-selection preference: a DIFFERENT app minting
     * that same dek_name first is rejected once enforce=true. Defaults to
     * false for the same reason ClassificationGovernance does -- kek_registry
     * already has real rows today, created before this check existed;
     * flipping to enforce=true immediately could reject a first-time mint
     * that collides with an existing row nobody intended as a reservation
     * conflict. Flip only after shadow-mode logging confirms no such
     * collision exists in current data.
     */
    public record DekNameReservation(
            boolean enforce
    ) {
    }
}
