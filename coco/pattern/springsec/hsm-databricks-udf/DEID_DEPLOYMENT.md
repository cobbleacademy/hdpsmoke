# Deploying the de-identification export (`hsm-deid`) to Databricks

`hsm-deid` reads a Delta table (or a query), de-identifies it with an approved
profile, and writes **one CSV and its manifest** to a Unity Catalog volume folder,
for the client's MFT to send to the vendor. It's the Databricks version of the
Kubernetes `DEID` job, with the same profiles, methods, output and surrogate
table. The design is in
[`java/docs/DEIDENTIFICATION.md`](../java/docs/DEIDENTIFICATION.md) section 17.

One wheel, one Python wheel task, on **serverless** or a **dedicated** job cluster.
Job definitions to start from are in [`jobs/`](jobs/).

> **First deployment: check the items in section 9.** The job is tested on local
> Spark 4.0 and Postgres 16 (`tests/deid/spark/run-tests.sh`), not yet on
> Databricks itself.

## 1. What you need

| Thing | What it's for | Section |
|---|---|---|
| An `app_id` in hsm-core for this job, its keypair and grants | Unwrapping keys and decrypting `decrypt: true` columns | 2 |
| A secret scope | hsm-core credentials, the surrogate database password | 3 |
| Three volumes (or folders) | The wheel, the profiles, the output per vendor | 4 |
| The surrogate table in Postgres or Lakebase | Only for profiles that use `surrogate` | 5 |
| Network egress to hsm-core and the database | Serverless with restricted egress needs both allowed | 6 |
| The keys, created once | Token key per vendor, the surrogate-table key | 7 |

## 2. Register the job in hsm-core

Register a dedicated `app_id`, for example `giftcard-deid-databricks`. Don't
reuse the encrypt/decrypt functions' `app_id`: the export has its own grants and
its own audit trail (`DEIDENTIFICATION.md` section 11.1).

1. **Scopes:** `dek_unwrap`, and `dek_issue` only if this job creates the keys
   (section 7). Nothing else.
2. **Transport keypair:** generate it unencrypted and register the public half,
   exactly as in [`DEPLOYMENT.md`](DEPLOYMENT.md) §0.2–0.3 (with this `app_id`).
3. **Auth:** `STATIC` (a bearer token) or `SELF_SIGNED_JWT` (§0.4 there).
4. **Decrypt grants:** one `app_dek_grants` row per encrypted source column's
   DEK, as for the Kubernetes job (`DEIDENTIFICATION.md` section 11.1). A missing
   grant fails the run before anything is written, naming the `edek_id`.

## 3. Secret scope

The driver reads secrets with `dbutils`; the workers never do. Store keys base64
encoded (`DEPLOYMENT.md` §0.2 explains why):

```bash
databricks secrets create-scope hsm
databricks secrets put-secret hsm deid-private-key --string-value "$(base64 -w0 deid-transport-key.pem)"
databricks secrets put-secret hsm deid-token --string-value "$TOKEN"            # STATIC
# databricks secrets put-secret hsm deid-signing-key --string-value "$(base64 -w0 deid-signing-key.pem)"   # SELF_SIGNED_JWT
# databricks secrets put-secret hsm deid-ca-bundle --string-value "$(base64 < ca.pem | tr -d '\n')"          # internal CA
databricks secrets put-secret hsm deid-surrogate-db-password --string-value "$DB_PASSWORD"
```

The job's run-as identity (a service principal) needs `READ` on the scope.

Pass the key **names** to `hsm-deid`, never the values: `--hsm-private-key-key`,
`--hsm-token-key` (or `--hsm-signing-key-key`), `--hsm-ca-cert-key`,
`--surrogate-db-password-key`. If you'd rather keep one JSON object with every
`HSM_*` field (as the functions' `credentials_json`), store it as one secret and
pass `--hsm-credentials-key` instead.

## 4. Volumes

| Volume (example) | Holds | Who writes it |
|---|---|---|
| `/Volumes/<catalog>/<schema>/libs` | `hsm_databricks_udf-<version>-py3-none-any.whl` | Release pipeline |
| `/Volumes/<catalog>/<schema>/deid/profiles` | Approved profiles, from version control | Release pipeline, after the onboarding team's approval |
| `/Volumes/<catalog>/<schema>/deid_out/<vendor>` | `<file>.csv`, `<file>.manifest.json` | The job only |

- **One output folder per vendor profile.** The client's MFT picks up from the
  storage behind it and must take `*.csv` only: the job writes `<file>.csv.part`
  first and renames it when complete.
- While running, the job also uses `<output root>/_deid_tmp/<run id>/` (Parquet
  and `.txt` files with no identifiers in them) and deletes it at the end, also on
  failure. Exclude `_deid_tmp/` from the MFT pick-up.
- The run-as identity needs `READ VOLUME` on `libs` and `profiles`, `READ VOLUME`
  and `WRITE VOLUME` on its output volume, and `SELECT` on the source table.
- An existing `<file>.csv` is never overwritten: a second run on the same day
  with a `{yyyyMMdd}` file name fails. Use `{run}` in `output.file` if a vendor
  takes several files a day.

Build and upload the wheel:

```bash
cd hsm-databricks-udf
python -m build      # dist/hsm_databricks_udf-<version>-py3-none-any.whl (needs setuptools >= 61)
databricks fs cp dist/hsm_databricks_udf-0.1.0-py3-none-any.whl \
  dbfs:/Volumes/<catalog>/<schema>/libs/hsm_databricks_udf-0.1.0-py3-none-any.whl
```

Upload profiles the same way, to the profiles volume.

## 5. The surrogate table (profiles using `surrogate` only)

The same table, from the same DDL, as the Kubernetes job:
[`java/hsm-bulk-client/src/main/resources/deid/deid-surrogate-postgres.sql`](../java/hsm-bulk-client/src/main/resources/deid/deid-surrogate-postgres.sql).
The job never creates or changes tables. Back it up with the owner's other Tier 0
data: without it, surrogates already sent can't be mapped back
(`DEIDENTIFICATION.md` section 16, "Surrogate retention").

```sql
-- as the database owner, once
\i deid-surrogate-postgres.sql
CREATE ROLE deid_export LOGIN PASSWORD '<password>';
GRANT SELECT, INSERT ON deid_surrogate TO deid_export;
```

- **Postgres** (`--surrogate-db postgres`): host, database, user and the password
  key. TLS is `--surrogate-db-sslmode require` by default.
- **Lakebase** (`--surrogate-db lakebase`): the instance's host and database
  (`databricks_postgres` by default) and a **Postgres role with a password**.
  OAuth tokens also work as the password, but they expire after about an hour and
  the workers can't fetch new ones, so a long run would fail part-way
  (`DEIDENTIFICATION.md` section 16, question 5).
- **Shared with the Kubernetes job:** if both jobs export for the same vendor,
  point them at the same table and use the same `surrogate-scope`: rows written
  by one are reused by the other (the cross-check proves the formats match).
- **Connections:** at most `--surrogate-partitions` (default 8) at once, plus one
  on the driver.

## 6. Network

- The **workers** call hsm-core (`/dek/unwrap`) and the surrogate database; the
  driver does too.
- **Serverless with restricted egress:** add both hosts to the workspace's
  allowed destinations (Settings → Network → Network policies), as for the
  functions (`DEPLOYMENT.md` §4).
- **Dedicated clusters:** the workspace's VNet must reach both, as for any
  external dependency.

## 7. Create the keys (once)

| Key | Name | Where its edek_id goes |
|---|---|---|
| Token key, one per vendor | `deid-<app>-<vendor>-tok` | The profile: `token-key-edek-id` |
| Surrogate-table key, one per data owner | `deid-<app>-surrogate` | The job: `--surrogate-key-edek-id` |

Run [`jobs/deid-init-key.json`](jobs/deid-init-key.json) once per key (edit
`--name`); the run output prints `{"edek_id": "…", "owner_app_id": "…",
"reused": false}`. It needs the `dek_issue` scope. Keys created by the Kubernetes
job's `--deid.init-key` work here too: they're the same named DEKs.

## 8. Create, dry-run and schedule the job

1. Start from [`jobs/deid-serverless.json`](jobs/deid-serverless.json) or
   [`jobs/deid-dedicated.json`](jobs/deid-dedicated.json). Replace every `<…>`.
   One job per vendor profile.
   - **Serverless:** `environments[].spec` installs the wheel and `psycopg`.
     Check `environment_version` against the versions your workspace offers.
   - **Dedicated:** a `SINGLE_USER` job cluster on a current LTS runtime
     (`spark_version`), the wheel and `psycopg` as task libraries. Size the
     workers for the table; the CSV itself is written by one task.
   - `max_retries` is 0: a failed run leaves no CSV and should be looked at, not
     retried blindly. Surrogate rows it inserted stay and are reused.
2. Create it: `databricks jobs create --json @jobs/deid-serverless.json`.
3. **Dry run first**, for every new or changed profile: run once with
   `--dry-run --sample 100` added to the parameters (run now with different
   parameters). It writes nothing (no CSV, no manifest, no surrogate rows) and
   prints a review: per field the method, null rate and distinct count in the
   sample, the passthrough fields, the columns dropped by default, and
   `ciphertext_not_marked_decrypt`. The onboarding team reviews it before the
   first real run.
4. Run it, check the CSV and manifest in the output folder, then unpause the
   schedule (the examples are monthly, paused).

**Logs:** the run's driver log has `deid_start`, one `deid_profile_problem` per
problem, and one JSON audit event on logger `hsm_databricks_udf.deid.audit`:
`deid_export_completed` (profile id, version and SHA-256, vendor, basis, rows,
file, file SHA-256, triggered by) or `deid_export_failed` (with the reason). No
values ever appear in them. Ship the job's logs to the SIEM like the other
audit sources.

## 9. First deployment: check these

Tested locally (Spark 4.0.1, Postgres 16), but not yet on Databricks:

| Check | How |
|---|---|
| The wheel task finds the entry point | A dry run starts and logs `deid_start`. The wheel's metadata is checked locally (both `hsm_databricks_udf` and `hsm-databricks-udf` resolve to the `hsm-deid` entry point); if the task still can't find it, try `package_name: hsm-databricks-udf` |
| Serverless: `mapInPandas`, Parquet and text writes to a volume path, reading the output back on the driver | The dry run and a first real run on a small table |
| Serverless: setting `spark.sql.session.timeZone` | Timestamps in a passthrough column read as UTC (`2026-11-02T10:15`) |
| `dbutils` from a wheel task (`databricks.sdk.runtime`, else `pyspark.dbutils`) | Secrets resolve; otherwise the task fails with "can't be read outside Databricks" |
| Workers reach hsm-core and the database | A profile with `decrypt: true` and `surrogate` completes |
| The rename on the volume | No `.csv.part` is left beside the `.csv` |
| Lakebase login with a password role | The surrogate step completes |

## 10. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `deid_profile_problem … problem=…`, then the run fails | The profile breaks a rule (basis, never-export field, unknown option, a field not in the source) | Fix the profile (pull request, the onboarding team's approval); dry-run it |
| `field 'x' holds HSM ciphertext but isn't decrypt: true` | An encrypted column exported without `decrypt: true` | Add `decrypt: true` (and its grant) or drop the field |
| `field 'x': decrypt failed for edek_id=…: …403…` / `no decrypt grant` | No grant on that column's DEK for this `app_id` | Section 2, step 4 |
| `field 'x', id 123: not an ISO date or date-time; set input-format` | A date column in another format | `input-format` on the field. The Python job reads `y u M d D H k K h m s S a` and `MMM`; other letters are refused when the profile is checked |
| `option 'input-format' uses pattern letter(s) …` | A pattern letter the Python job doesn't read (for example `EEE`, `MMMM`) | Change the pattern, or select the column through `--source-query` already converted |
| `column 'x' is binary; it can't be written to a CSV` (or `is a array`, `a struct`) | A binary or complex column the profile exports | Convert it in `--source-query` |
| `the surrogate table … is missing or unusable` | No table, wrong schema, or no `SELECT` | Section 5 |
| `could not find an unused surrogate after 5 tries` | The surrogate space for the scope is nearly full | Raise the field's `length` (a new series of surrogates for new values only) |
| `output file … already exists; not overwriting it` | A second run with the same file name | Use `{run}` in `output.file`, or move the earlier file once sent |
| `--surrogate-key-edek-id is required` | A `surrogate` profile without the key | Section 7 |
| `secret '…' can't be read outside Databricks` | `dbutils` isn't available to the task | See section 9 |
| Connection timeout to hsm-core or the database | Egress | Section 6 |
| `ImportError: psycopg` | The task's environment or libraries miss `psycopg[binary]` | Section 8, step 1 |

A failed run never leaves a `.csv`: the `.part` file and the temporary folder are
deleted. Re-run once the cause is fixed; members inserted into the surrogate
table by the failed run keep their surrogates.
