package com.hsm.client.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * Typed config for hsm-bulk-client. Deliberately narrow -- this module has no server
 * side at all, so unlike HsmBulkProperties/HsmProperties there's no jwt/azure-kek
 * section here: authenticating TO SVC (hsm-bulk-service) is just a bearer token +
 * X-App-ID header (svc.token/svc.app-id below), not a JWT this module validates.
 */
@ConfigurationProperties(prefix = "client")
public record ClientProperties(
        Job job,
        Svc svc,
        Db db,
        File file
) {

    /** Which of the two jobs to run, and in which direction. One process, one job, one direction, per invocation. */
    public record Job(
            Type type,
            Mode mode
    ) {
        public enum Type { DB, FILE }
        public enum Mode { ENCRYPT, DECRYPT }
    }

    public record Svc(
            String baseUrl,
            String apiV1Prefix,    // must match SVC's own API_V1_PREFIX (hsm.service.api-v1-prefix) -- the two are configured independently and not auto-synced
            String appId,
            AuthMode authMode,     // STATIC (default) uses token below, unchanged; AZURE_AD acquires a fresh bearer token per call via Workload Identity -- see AzureAdTokenProvider
            String token,          // only used when authMode=STATIC -- a real Azure AD JWT here would expire mid-job on any run longer than its TTL
            String azureTokenScope, // only used when authMode=AZURE_AD -- must match whatever SVC's own Azure AD app registration exposes as its audience/scope
            int dekBatchMaxItems,  // mirrors hsm.service.dek-batch-max-items on SVC -- self-limit client-side rather than rely on SVC's 422 rejection
            String privateKeyPem   // PKCS#8 PEM, the private half of the public key registered on app_registrations.public_key_pem for appId -- never sent anywhere, only used locally to unwrap what SVC returns
    ) {
        public enum AuthMode { STATIC, AZURE_AD }
    }

    public record Db(
            TableRef source,
            TableRef target,
            String keyColumn,
            List<ColumnMapping> columns,
            // Non-sensitive columns copied source-to-target as-is, never encrypted/decrypted
            // -- same name in both tables (no renaming, unlike columns above). Deliberately
            // explicit, not auto-discovered from the source table's schema: if a source
            // column later becomes sensitive, it simply won't appear here (fetchPage/
            // insertRows in DbBulkJob never reference it) rather than being silently
            // copied in plaintext into what may be a "secure" target table.
            List<String> passthroughColumns,
            int rowBatchSize
    ) {
        public record TableRef(String jdbcUrl, String username, String password, String schema, String table) {
        }

        /**
         * targetType only matters for DECRYPT jobs -- it's the SQL type the decrypted
         * plaintext should be parsed into before insertion, since DekManager.decrypt()
         * always hands back raw UTF-8 bytes as a String regardless of the original
         * column's type. Ignored on ENCRYPT (target there is always the ciphertext_token
         * VARCHAR/TEXT column). Null/unset -> STRING, the pre-existing behavior.
         *
         * <p>Only as many types as there are genuinely distinct JDBC parameter
         * conversions needed, not one enum value per SQL dialect's type name --
         * NUMERIC covers DECIMAL/NUMERIC-family columns, INTEGER covers
         * INT/BIGINT/SMALLINT-family columns.
         */
        /**
         * dekName is optional and independent of targetType: unset -&gt; today's default,
         * one DEK issued per (row, column) value. Set -&gt; every row's value for this
         * column shares the current DEK for (svc.app-id, dekName) instead of each
         * getting its own -- one /dek/issue lookup per column per job run instead of
         * one per row. Same name must be used on both the ENCRYPT job's and the
         * DECRYPT job's config for this column (mirrors how targetType only matters
         * on the DECRYPT side -- dekName only matters on the ENCRYPT side, since
         * DECRYPT resolves purely by the edek_id embedded in each row's own token).
         */
        public record ColumnMapping(String source, String target, TargetType targetType, String dekName) {
            public enum TargetType { STRING, DATE, TIMESTAMP, NUMERIC, INTEGER }
        }
    }

    public record File(
            StoreRef source,
            StoreRef target,
            List<String> fileTypes,
            int chunkSizeBytes,
            int filesPerBatch
    ) {
        public record StoreRef(StoreType type, String root) {
        }

        public enum StoreType { LOCAL, ADLS }
    }
}
