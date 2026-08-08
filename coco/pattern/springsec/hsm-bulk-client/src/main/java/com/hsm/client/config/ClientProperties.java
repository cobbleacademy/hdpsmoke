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
            // Only used to parse a persisted checkpoint's last_key (stored as TEXT) back
            // into the correct Java type for the WHERE key_column > ? resume parameter --
            // irrelevant when checkpoint.enabled is false. Reuses ColumnMapping.TargetType
            // rather than a second enum; null/unset -> STRING, same default as targetType.
            ColumnMapping.TargetType keyColumnType,
            List<ColumnMapping> columns,
            // Non-sensitive columns copied source-to-target as-is, never encrypted/decrypted
            // -- same name in both tables (no renaming, unlike columns above). Deliberately
            // explicit, not auto-discovered from the source table's schema: if a source
            // column later becomes sensitive, it simply won't appear here (fetchPage/
            // insertRows in DbBulkJob never reference it) rather than being silently
            // copied in plaintext into what may be a "secure" target table.
            List<String> passthroughColumns,
            int rowBatchSize,
            Checkpoint checkpoint,
            // 1 (default, and what an omitted/0 value means) -- today's exact
            // sequential behavior, unchanged. >1 partitions the key range into that
            // many pieces up front and runs one independent worker per partition
            // concurrently; each worker does the identical fetch-issue-encrypt-insert
            // pipeline scoped to its own key range. See DbBulkJob's class javadoc for
            // how partitioning stays cheap (a bounded, one-time set of boundary
            // lookups, not per-page OFFSET pagination).
            int parallelism
    ) {
        public Db {
            if (parallelism <= 0) {
                parallelism = 1;
            }
        }

        public record TableRef(String jdbcUrl, String username, String password, String schema, String table) {
        }

        /**
         * Persists progress (the last successfully committed key-column value) so a
         * crashed or killed run can resume past what was already written instead of
         * restarting from the first row. Deliberately separate from the source/target
         * key-uniqueness question discussed alongside this feature -- the checkpoint
         * table owns its own guaranteed-unique job_id key, and DbBulkJob commits each
         * data batch and its checkpoint update in one transaction, so this works without
         * requiring an upsert (or a unique constraint) on the actual target table.
         *
         * <p>enabled=false (default, and what an omitted checkpoint block means) --
         * unchanged pre-existing behavior, no checkpoint table touched at all.
         * resume=true reads the last committed key and continues past it; resume=false
         * ("override") clears any prior progress for jobId and starts from the
         * beginning, then records progress going forward so a later resume=true run
         * could pick up from here. jobId must be unique per distinct job pipeline --
         * two different jobs sharing one checkpoint table must not reuse the same
         * jobId or they will clobber each other's progress. No separate "complete"
         * flag is needed: resuming after a fully-finished run just fetches zero rows
         * past the last key and exits immediately, same as a fresh run of an empty
         * table would.
         */
        public record Checkpoint(
                boolean enabled,
                boolean resume,
                String jobId,
                String tableName
        ) {
            public Checkpoint {
                if (tableName == null || tableName.isBlank()) {
                    tableName = "hsm_bulk_checkpoint";
                }
            }
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
            int filesPerBatch,
            // Unset (default) -- today's exact behavior, every file mints its own DEK.
            // Set -- one persistent DEK for this job's business purpose, resolved once
            // via /dek/issue and reused across every future run that uses the same
            // name (not just within one run, unlike DbBulkJob's per-column dek-name).
            // Needs hsm-bulk-service's own named-DEK rotation (see
            // com.hsm.bulk.scheduler.NamedDekRotationScheduler) to bound how much data
            // one long-lived name ends up protecting.
            String dekName,
            // 1 (default) -- today's exact sequential behavior. >1 partitions the file
            // list into that many groups and runs one independent worker per group
            // concurrently, each running the same per-file pipeline.
            int parallelism,
            Checkpoint checkpoint
    ) {
        public File {
            if (parallelism <= 0) {
                parallelism = 1;
            }
        }

        public record StoreRef(StoreType type, String root) {
        }

        public enum StoreType { LOCAL, ADLS }

        /**
         * Tracks per-file completion via a single batched manifest file in the target
         * FileStore (not a marker-per-file, which would double write/blob-create
         * operations -- real added cost on ADLS specifically -- and not a new external
         * DB dependency, keeping this job storage-agnostic same as everything else
         * here). Same enabled/resume/job-id shape and semantics as Db.Checkpoint --
         * enabled=false (default) touches no manifest at all.
         */
        public record Checkpoint(
                boolean enabled,
                boolean resume,
                String jobId,
                // How many file completions between manifest rewrites. Deliberately its
                // own setting, not tied to filesPerBatch (that's an HTTP-batching-size
                // knob, unrelated) -- each flush rewrites the whole manifest (FileStore
                // has no append primitive), so its cost grows with total completed count.
                // A too-small interval multiplies that cost badly at scale: measured
                // directly at filesPerBatch=10 on 60k files, that coupling made a resume
                // take 58s instead of the ~1s the same file count takes uncoupled. 1000
                // (default, unset/<=0 falls back to it) keeps the number of rewrites, and
                // therefore their total cost, bounded regardless of filesPerBatch.
                int flushInterval
        ) {
            public Checkpoint {
                if (flushInterval <= 0) {
                    flushInterval = 1000;
                }
            }
        }
    }
}
