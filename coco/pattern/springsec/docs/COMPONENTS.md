# Components at a glance

What each part of this repository is, how it runs and what it talks to, with a
link to where the details live. Architecture and design are in the linked
docs; this page is the map.

## The system in one paragraph

Applications encrypt sensitive data through **hsm-core-service**, which uses
**envelope encryption**. Each record or file is encrypted with its own data key
(**DEK**). The DEK is stored only in wrapped form, encrypted under a master key
(**KEK**) that never leaves Azure Key Vault Managed HSM. Callers either:

- **Tier 1:** send data to core and get ciphertext back (`/encrypt`, `/decrypt`); or
- **Tier 3:** ask core for a DEK (`/dek/issue`, `/dek/unwrap`) and encrypt locally,
  for bulk volumes.

Everything else in the repo is a client, worker, library or deployment of that
one service.

| Term | Meaning |
|---|---|
| **KEK** | Master key in the Managed HSM. Wraps DEKs. Never leaves the HSM. |
| **DEK** | Data key (AES-256). Encrypts records and files. |
| **CEK** | *Cache* Encryption Key. Encrypts DEKs while they sit in Redis. Not a data key. |
| **`app_id`** | A calling application's identity in core, with its scopes and public key |
| **Grant** | Permission for one `app_id` to use another's keys |

## Component map

```
                      ┌────────────────────── Azure ──────────────────────┐
                      │  Managed HSM (KEK)   Key Vault secrets (CEK)      │
                      └───────────▲────────────────────▲──────────────────┘
                                  │                    │
 apps ── Tier 1 ──► hsm-core-service ◄── Redis ◄── hsm-cache-key-rotator
 (UI, APIs)         (releases: core,      (DEK cache)
                     bulk, demo)
                         ▲  ▲  Postgres (key records, app registrations)
            Tier 3       │  │
 ┌───────────────────────┘  └──────────────────────────┐
 hsm-bulk-client (batch)   hsm-file-service (serve)    hsm-spark-adapter / hsm-databricks-udf
        └──── built on hsm-crypto-client + hsm-file-store ──┘      (built on crypto-client / Python port)
```

## Services (long-running)

### hsm-core-service
The central encryption service; everything else depends on it.

| | |
|---|---|
| **Does** | Encrypt/decrypt (Tier 1); issue/unwrap DEKs (Tier 3); key rotation; cross-app grants; app admin |
| **API** | `/encrypt`, `/encrypt/batch`, `/decrypt`, `/decrypt/batch`, `/dek/issue`, `/dek/unwrap`, `/admin/*`. JSON, snake_case. |
| **Auth** | Entra ID JWT, self-signed app JWT, or mTLS client certificate, plus `X-App-ID`. Per-endpoint scopes in `hsm.security.access-rules`. |
| **Depends on** | Managed HSM (KEK), Postgres (key records, app registrations), Key Vault secrets (CEK), Redis (optional DEK cache), Splunk HEC (audit), PlainID (optional policy check) |
| **Runs as** | Deployment, `helm/hsm-core-service`, port 3005. **One image, several releases:** |

| Release | Path prefix (external) | What it is |
|---|---|---|
| **core** | `/api/dsec/core/v1` | Main production release |
| **bulk** (`helm/hsm-bulk-service`) | `/api/dsec/bulk/v1` | Same image, scaled separately to isolate bulk (Tier 3) traffic |
| **demo** | `/api/sensec/hsm/v1` | Same image with `DEMO_MODE=true`: H2, mock HSM, demo tokens, demo UI, Swagger on. No Azure needed. |

The VirtualService rewrites the external prefixes to the internal
`/api/sensec/hsm/v1`. Swagger works through every prefix because the service
emits only relative URLs (`<prefix>/docs`).

**Docs:** `AUTHORIZATION.md`, `ADMIN_OPERATIONS.md`, `APP_ONBOARDING.md`,
`BULK_OPERATIONS.md`, `CACHING_AND_ROTATION.md`, `RUNBOOK.md`, `DISASTER_RECOVERY.md`

### hsm-cache-key-rotator
Background worker that rotates the **CEK**, the key protecting core's Redis DEK
cache. (Formerly `cek-rotation-service`.)

| | |
|---|---|
| **Does** | Writes a fresh CEK to the inactive Key Vault slot (`cek-alpha`/`cek-beta`), flips `cek-current-key`, and re-keys or flushes Redis. Core picks up the new key without a restart. |
| **Depends on** | Key Vault secrets, Redis |
| **Runs as** | Deployment with no HTTP port, `helm/hsm-cache-key-rotator`. Default interval 4 h. |
| **Docs** | `CACHING_AND_ROTATION.md` |

### hsm-file-service
Read-only service that runs **in a consumer's namespace** and serves decrypted
files to that consumer's BFF (backend-for-frontend).

| | |
|---|---|
| **Does** | `GET /api/sensec/file/v1/files/{path}` (prefix configurable, `config.server.apiPrefix`): reads an encrypted file, verifies every chunk, returns the original bytes. Small files are verified before sending; large ones stream, and the connection is cut if tampering is detected mid-stream. |
| **Auth** | Istio mTLS + AuthorizationPolicy (BFF only); no tokens. It unwraps keys from core as its own `app_id`, under a cross-app grant. |
| **Depends on** | hsm-core-service (`/dek/unwrap`), Azure Blob/ADLS (read-only) |
| **Runs as** | Deployment, `helm/hsm-file-service`. Port 8080 (files; plus Swagger UI at `<prefix>/docs` in dev/test with `docs.enabled`), 8081 (probes, metrics, OpenAPI; never routed through a VirtualService). Core team ships the image and chart; the consumer configures them. |
| **Docs** | `FILE_SERVICE.md`, `FILE_FORMAT.md`; contract `helm/hsm-file-service/openapi.yaml` |

## Batch and test jobs

### hsm-bulk-client
Batch job that encrypts or decrypts data **in bulk** using Tier 3 (local AES-GCM
with DEKs from core).

| | |
|---|---|
| **Does** | **DB jobs:** encrypt/decrypt columns across tables (Postgres, SQL Server, Oracle). **File jobs:** encrypt/decrypt files between Local, ADLS and Blob. Checkpoint/resume, parallelism, and for v2 files, per-batch result files (path → `file_id`). |
| **Configure** | One `job.yml` per run: source, target, direction, auth mode. See `hsm-bulk-client/config-examples/`. |
| **Runs as** | Kubernetes Job (`helm/hsm-bulk-client-job`), or `java -jar` locally. Runs, then exits. |
| **Docs** | `BULK_OPERATIONS.md`, `TIER3_POC_BUILD.md`, `FILE_FORMAT.md` |

### hsm-core-service-loadtest
Gatling load test for core's `/encrypt` and `/decrypt`.

| | |
|---|---|
| **Runs as** | Kubernetes Job (`helm/hsm-core-service-loadtest-job`); image `Dockerfile.hsm-core-service-loadtest` |
| **Docs** | `PERFORMANCE_TESTING.md` |

## Libraries (not deployed on their own)

### hsm-crypto-client
The shared Java client library. **All Java callers of core go through it.**

| | |
|---|---|
| **Provides** | `HsmCryptoClient` (encrypt/decrypt, file encrypt/decrypt); the chunked encrypted-file codec (v1/v2); AES-GCM and RSA-OAEP primitives on BC-FIPS; bounded DEK cache; the HTTP client for core; all four auth modes (static, Azure AD, self-signed JWT, mTLS) |
| **Used by** | hsm-bulk-client, hsm-file-service, hsm-spark-adapter, and any JVM app embedding it |
| **Docs** | `TIER3_POC_BUILD.md`, `FILE_FORMAT.md`, `AUTHORIZATION.md` |

### hsm-file-store
Storage adapters behind one `FileStore` interface: **Local disk, ADLS Gen2,
Azure Blob**. Used by hsm-bulk-client and hsm-file-service. Kept separate from
the crypto library so the Azure Storage SDKs never reach embedders such as the
Spark adapter.

## Data-platform integrations

| Component | What it is | Use it when | Docs |
|---|---|---|---|
| **hsm-spark-adapter** | `hsm_encrypt` / `hsm_decrypt` as Spark SQL functions (a JVM extension built on hsm-crypto-client) | Spark jobs on dedicated clusters | `SPARK_ADAPTER.md` |
| **hsm-databricks-udf** (repo root, Python) | Same functions as Unity Catalog Python UDFs | Databricks shared clusters, serverless, DLT (where JVM extensions aren't allowed) | `DATABRICKS_UDF_DESIGN.md`, `hsm-databricks-udf/DEPLOYMENT.md` |
| **spark-verification-app** (repo root) | Standalone local Spark session that exercises the Spark adapter against a real core | Verifying the adapter without a cluster | its `README.md` |

## Client examples

In `hsm-bulk-client/examples/`: **Python** and **.NET** reference clients. They
show how to call core directly (Tier 1 file chunking), and how to read or
rescue a bulk-encrypted file through core's `/decrypt` alone. They are copied
into consumer code, not deployed. See the `README.md` in each folder.

## Legacy (Python)

The original Python implementation, kept for reference and rollback. It is not
built or deployed by the Helm charts.

| Path | What |
|---|---|
| `app/` | The original FastAPI encryption service (the Java core is its port) |
| `cek_rotation/` | Original CEK rotation worker (the Java rotator is its port) |
| `scheduler/`, `migrations/`, `docs/DEMO.md` | Original KEK-rotation job, Alembic migrations, demo guide |

## Which one do I use?

| I want to… | Use |
|---|---|
| Encrypt/decrypt a few values from an app | **hsm-core-service** Tier 1 API, or **hsm-crypto-client** from a JVM app |
| Encrypt/decrypt millions of DB rows or many files | **hsm-bulk-client** job |
| Let a UI download encrypted files as plaintext | **hsm-file-service** in the consumer's namespace |
| Encrypt/decrypt in Spark SQL / Databricks | **hsm-spark-adapter** (JVM clusters) / **hsm-databricks-udf** (shared, serverless) |
| Recover a file without the Java tooling | Python/.NET example readers through core's `/decrypt` |
| Try it all without Azure | the **demo** release (`DEMO_MODE=true`) |

## Where things live

| Kind | Path |
|---|---|
| Java modules (Maven reactor) | `java/` (parent `java/pom.xml`) |
| Container images | `java/docker/Dockerfile.<component>` |
| Helm charts | `helm/<component>/` |
| Design and operations docs | `java/docs/` (start with `RUNBOOK.md` for incidents) |
| Diagrams | `java/docs/architecture-diagram.svg`, `sequence-diagram.svg` |
