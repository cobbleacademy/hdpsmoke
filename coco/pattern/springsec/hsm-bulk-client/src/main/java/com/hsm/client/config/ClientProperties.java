package com.hsm.client.config;

import com.hsm.client.db.DbDialect;
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
            // false (default, unset) -- today's exact behavior: keyColumn's raw source
            // value is always the first column of every target INSERT, using keyColumn's
            // own name as the target column name too. Set true when keyColumn is a
            // pagination-only value with no home in the target row at all -- e.g. a
            // ROW_NUMBER() computed by TableRef.query (see its javadoc), or any keyColumn
            // whose name doesn't exist as a real column on the target table. Target rows
            // are then built purely from columns + passthroughColumns; if the target
            // table has its own key column, it's expected to populate itself (IDENTITY/
            // SERIAL/AUTO_INCREMENT), not receive a value from this job.
            boolean skipKeyColumnOnTarget,
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

        // dialect is null (auto-detect from jdbcUrl's scheme) unless explicitly set --
        // needed because a JDBC URL scheme identifies which driver/wire protocol is in
        // use, not necessarily which real SQL dialect quirks the backend has (e.g. a
        // product that presents another vendor's wire protocol for driver
        // compatibility while diverging from that vendor's actual SQL behavior). See
        // DbDialect's own javadoc.
        //
        // <p>query is an alternative to table, meaningful only on source (target is
        // always a real table DbBulkJob writes INSERTs into -- query is simply never
        // read for config.target()). When set, DbBulkJob wraps it as a derived table
        // ("(" + query + ") AS src") and paginates against that instead of a plain
        // table name -- the query re-executes fresh on every page fetch, nothing is
        // persisted, so no CREATE VIEW privilege is needed on the source database.
        // The main use: a source with no existing non-sensitive, stable, sortable
        // column to use as keyColumn can compute one inline, e.g.
        // "SELECT ROW_NUMBER() OVER (ORDER BY ssn) AS row_key, ssn, dob FROM
        // dbo.customers" with keyColumn=row_key -- see BULK_OPERATIONS.md. Only
        // stable across pages if the underlying rows aren't concurrently
        // inserted/deleted/reordered during the run; table and query are mutually
        // exclusive -- set exactly one.
        //
        // <p>REQUIREMENT, not a preference: ROW_NUMBER()'s ORDER BY must be over a
        // genuinely unique column or column combination, with zero duplicate values
        // in the actual data -- not "probably unique," verified unique. There is
        // nothing persisted here (unlike a real key column's stored value): every
        // fetchPage() call, every computePartitionRanges() boundary lookup, is an
        // independent re-execution that recomputes ROW_NUMBER() from scratch. A
        // fully unique ORDER BY has exactly one correct numbering regardless of how
        // many times or in what order the query re-runs. A non-unique ORDER BY does
        // not -- ties have no defined order in SQL, and while a given engine often
        // (not always) returns the same tie order for repeated identical queries on
        // unchanged data, that is an implementation detail, not a guarantee -- it
        // can break under a different query plan (e.g. parallel internal query
        // execution, common for large scans on Postgres/SQL Server) with no schema
        // or data change at all. When that happens a tied row can be renumbered
        // between two calls, so it lands in the wrong partition or the wrong side
        // of a keyset WHERE row_key > ? boundary -- silently skipped or processed
        // twice, in both the parallelism=1 (page-to-page) and parallelism>1
        // (cross-worker) cases equally; this is not specific to either. Verified
        // empirically (not just reasoned about) in DerivedTableParallelismTest.
        public record TableRef(String jdbcUrl, String username, String password, String schema, String table, String query, DbDialect dialect) {
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
                String tableName,
                // Unqualified (default/unset) means CREATE TABLE hsm_bulk_checkpoint
                // resolves against whatever schema is first in the connecting role's
                // default search_path -- which is not guaranteed to be one that role
                // can actually create in (a real failure mode: "permission denied for
                // schema X" for some X that isn't source/target.schema at all). Set
                // this explicitly, typically matching target.schema, to control
                // exactly where the checkpoint table lands instead of relying on the
                // connection's default search_path.
                String schema
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
         * column's type. Ignored on ENCRYPT (target there is always the ciphertext
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

        // accountKey is null/blank (default) in every real deployment -- ADLS/AZURE_BLOB
        // then resolve credentials purely via the WorkloadIdentityCredential ->
        // ManagedIdentityCredential -> DefaultAzureCredential chain, same as before this
        // field existed. Setting it makes AdlsFileStore/AzureBlobFileStore use
        // StorageSharedKeyCredential instead, bypassing that chain entirely -- a
        // deliberate, explicit escape hatch for validating encrypt/decrypt against a
        // real ADLS/Blob container BEFORE the deployment identity's RBAC data-plane role
        // (Storage Blob Data Contributor) is actually granted/propagated, since that's a
        // separate Azure-side fix this job can't itself unblock. Never logged. Remove
        // this field from the job config entirely for the real deployment run -- its
        // presence at all, not just a boolean toggle, is what activates shared-key auth,
        // so an accidentally-left-in value would silently keep bypassing WorkloadIdentity
        // even in production. LOCAL ignores it (no Azure credential to resolve).
        public record StoreRef(StoreType type, String root, String accountKey) {
        }

        // ADLS: real ADLS Gen2 (Hierarchical Namespace enabled), root is
        // abfss://<container>@<account>.dfs.core.windows.net/<path> -- see
        // AdlsFileStore. AZURE_BLOB: plain Azure Blob Storage (no HNS
        // required, and unlike ADLS Gen2's Data Lake REST API, has no known
        // conflict with the account-level "soft delete for blobs" feature --
        // root is https://<account>.blob.core.windows.net/<container>/<path>,
        // a real blob endpoint URL, deliberately not reusing abfss:// since
        // that scheme means "Data Lake Gen2 API" specifically -- see
        // AzureBlobFileStore. Existing ADLS deployments are unaffected; this
        // is an additional option for accounts that either can't or don't
        // have HNS enabled.
        public enum StoreType { LOCAL, ADLS, AZURE_BLOB }

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
