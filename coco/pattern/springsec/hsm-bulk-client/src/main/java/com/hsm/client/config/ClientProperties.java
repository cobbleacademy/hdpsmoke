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
            String token,
            int dekBatchMaxItems,  // mirrors hsm.service.dek-batch-max-items on SVC -- self-limit client-side rather than rely on SVC's 422 rejection
            String privateKeyPem   // PKCS#8 PEM, the private half of the public key registered on app_registrations.public_key_pem for appId -- never sent anywhere, only used locally to unwrap what SVC returns
    ) {
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

        public record ColumnMapping(String source, String target) {
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
