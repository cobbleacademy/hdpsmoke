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
> Databricks itself. Section 11 is a step-by-step first test on a workspace,
> with its own test table and profiles in [`jobs/test/`](jobs/test/).

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
3. **Auth:** one of two, as in `DEPLOYMENT.md` §0 step 4:
   - **`STATIC`:** a bearer token for this `app_id`, stored as `deid-token`.
   - **`SELF_SIGNED_JWT`:** the job signs its own short-lived token on every
     call, so there's no token to renew. Generate a second keypair just for
     signing (unencrypted) and register its public half as
     `signing_public_key_pem`:
     ```bash
     openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out deid-signing-key.pem
     openssl pkey -in deid-signing-key.pem -pubout -out deid-signing-key.pub.pem
     curl -X POST "$BASE/admin/apps/keys" \
       -H "Authorization: Bearer $OPS_ADMIN_TOKEN" -H "X-App-ID: ops-admin" \
       -H "Content-Type: application/json" \
       -d "{\"app_id\": \"giftcard-deid-databricks\", \"signing_public_key_pem\": \"$(cat deid-signing-key.pub.pem)\"}"
     ```
     The token's audience is `hsm-core-service`. If hsm-core's
     `hsm.jwt.audience` is something else, the separate-secret flags can't set
     it: use the one-JSON form (`--hsm-credentials-key`, section 3) with
     `HSM_SELF_SIGNED_AUDIENCE` in it.
4. **Decrypt grants:** one `app_dek_grants` row per encrypted source column's
   DEK, as for the Kubernetes job (`DEIDENTIFICATION.md` section 11.1). A missing
   grant fails the run before anything is written, naming the `edek_id`.

## 3. Secret scope

The driver reads secrets with `dbutils`; the workers never do. Store keys base64
encoded (`DEPLOYMENT.md` §0.2 explains why):

```bash
databricks secrets create-scope hsm
databricks secrets put-secret hsm deid-private-key --string-value "$(base64 -w0 deid-transport-key.pem)"
databricks secrets put-secret hsm deid-token --string-value "$TOKEN"                                  # STATIC only
databricks secrets put-secret hsm deid-signing-key --string-value "$(base64 -w0 deid-signing-key.pem)"   # SELF_SIGNED_JWT only
databricks secrets put-secret hsm deid-ca-bundle --string-value "$(base64 < ca.pem | tr -d '\n')"            # hsm-core's CA
databricks secrets put-secret hsm deid-surrogate-db-password --string-value "$DB_PASSWORD"
```

The job's run-as identity (a service principal) needs `READ` on the scope.

### 3.1 hsm-core's API version and TLS certificate

The job files call hsm-core's **API v2** and trust hsm-core's **CA bundle** from
the scope. Both are parameters only, no code change:

| Parameter | Value | Notes |
|---|---|---|
| `--hsm-base-url` | `https://<host>/api/sensec/hsm/v2` | Ends in the version |
| `--hsm-api-version` | `V2` | Must match the URL's ending; the job refuses `V2` with a `/v1` URL (and the reverse) before calling hsm-core |
| `--hsm-ca-cert-key` | `deid-ca-bundle` | The secret holding the CA certificate(s) that signed hsm-core's TLS certificate, PEM or base64 of PEM; certificates only, no private key |

- **V1:** change both together, `…/api/sensec/hsm/v1` and `--hsm-api-version V1`
  (the default if the flag is left out).
- **CA bundle:** needed whenever hsm-core's certificate comes from an internal
  CA; without it every call fails with `CERTIFICATE_VERIFY_FAILED`. It's added to
  the public CAs, not used instead of them, and certificate and host-name checks
  can't be turned off. Put the whole chain in the PEM
  (intermediate and root) if the server doesn't send its intermediate.
- **One-JSON form:** with `--hsm-credentials-key`, the same three values are the
  JSON's `HSM_SERVICE_BASE_URL`, `HSM_API_VERSION` and `HSM_CA_CERT_PEM` fields,
  and the separate flags aren't used.
- The driver reads the CA secret and passes it to the workers with the other
  credentials; the workers don't read the scope.

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

Build, check and upload the wheel, from `hsm-databricks-udf/` on the release
branch:

```bash
cd hsm-databricks-udf
rm -rf build dist src/*.egg-info          # no output from an earlier build mixed in
python -m pip install --upgrade build
python -m build --wheel                   # dist/hsm_databricks_udf-0.1.0-py3-none-any.whl

W=dist/hsm_databricks_udf-0.1.0-py3-none-any.whl
unzip -l "$W" | grep -q 'hsm_databricks_udf/deid/restricted-zip3.txt' \
  && unzip -p "$W" 'hsm_databricks_udf-0.1.0.dist-info/entry_points.txt' | grep -q 'hsm-deid = hsm_databricks_udf.deid.cli:main' \
  && echo "wheel OK" || echo "WHEEL BROKEN: don't upload it"

databricks fs cp "$W" dbfs:/Volumes/<catalog>/<schema>/libs/hsm_databricks_udf-0.1.0-py3-none-any.whl
```

Upload only after `wheel OK`. A wheel without `entry_points.txt` fails the job
with "entry point not found". Two ways to get one:

- **`--no-isolation`:** don't use it. It builds with the setuptools installed in
  that Python and ignores `requires = ["setuptools>=68"]`; setuptools older than 61
  ignores the whole `[project]` table, so the wheel has no entry point, no
  packages, and is named `UNKNOWN-0.0.0-py3-none-any.whl`. Without the flag,
  `build` installs a current setuptools in a temporary environment.
- **An old wheel of the same name:** every build is `0.1.0`, so a wheel from
  before the DEID code (no `hsm_databricks_udf/deid/` folder) has the same file
  name. pip also keeps one in its cache (`pip cache list hsm_databricks_udf`;
  clear it with `pip cache remove hsm_databricks_udf`). The `rm -rf` and the check
  above catch both.

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

Run the init-key job once per key (edit `--name`):
[`jobs/deid-init-key.json`](jobs/deid-init-key.json) for `STATIC` auth, or
[`jobs/deid-init-key-jwt.json`](jobs/deid-init-key-jwt.json) for
`SELF_SIGNED_JWT` (`--hsm-signing-key-key` instead of `--hsm-token-key`; the
rest is the same). The run output prints `{"edek_id": "…", "owner_app_id": "…",
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

Tested locally (Spark 4.0.1, Postgres 16), but not yet on Databricks. Section 11
walks through them in order on a test schema:

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
| `SSL: CERTIFICATE_VERIFY_FAILED` calling hsm-core | hsm-core's certificate is from an internal CA the job doesn't trust | Put the CA bundle in the scope and pass `--hsm-ca-cert-key` (section 3.1); include the intermediate if the server doesn't send it |
| `HSM_CA_CERT_PEM has no '-----BEGIN CERTIFICATE-----' block` / `contains a private key` | The CA secret holds the wrong file | Store the CA certificate(s) only, PEM or base64 of PEM |
| `HSM_API_VERSION is V2 but HSM_SERVICE_BASE_URL ends in /v1` (or the reverse) | `--hsm-api-version` and the URL don't match | Change both together (section 3.1) |
| `/dek/unwrap -> 400: …` or `/dek/issue -> 400: …` | A V1/V2 mix-up behind a gateway path that doesn't end in `/v1` or `/v2`, so it couldn't be checked | Set `--hsm-api-version` to the version that path serves |
| `/dek/issue -> 401: Invalid token signature` (`SELF_SIGNED_JWT`) | The signing key in `deid-signing-key` doesn't match the `signing_public_key_pem` registered for this `app_id`, or the private and signing keys are swapped in the scope | `openssl pkey -in <the signing key file> -pubout` and register that public key again (section 2, step 3) |
| `/dek/issue -> 401` with the right signing key (`SELF_SIGNED_JWT`) | hsm-core's `hsm.jwt.audience` isn't `hsm-core-service` | Use `--hsm-credentials-key` with `HSM_SELF_SIGNED_AUDIENCE` (section 2, step 3) |
| `HSM_SIGNING_PRIVATE_KEY_PEM was provided but empty` | The `--hsm-signing-key-key` secret is empty | Put the signing key in it (section 3) |
| Connection timeout to hsm-core or the database | Egress | Section 6 |
| The task fails: entry point `hsm-deid` not found, or `No module named 'hsm_databricks_udf.deid'` | The volume has a broken or old wheel (no `entry_points.txt`, or from before the DEID code) | Rebuild and check it as in section 4 (`wheel OK`), then upload it again |
| `ImportError: psycopg` | The task's environment or libraries miss `psycopg[binary]` | Section 8, step 1 |

A failed run never leaves a `.csv`: the `.part` file and the temporary folder are
deleted. Re-run once the cause is fixed; members inserted into the surrogate
table by the failed run keep their surrogates.

## 11. First test on a workspace, step by step

Tests one dependency at a time: Spark and the volume first, then the job
wrapper, then hsm-core and the surrogate database. When a stage fails, the
previous one tells you what already works. The files are in
[`jobs/test/`](jobs/test/); replace `<catalog>` everywhere. Everything goes in
its own `deid_test` schema, with made-up values only.

| File | What it is |
|---|---|
| [`jobs/test/giftcards.sql`](jobs/test/giftcards.sql) | The schema, three volumes and a four-row source table |
| [`jobs/test/plain.yml`](jobs/test/plain.yml) | A profile that needs no hsm-core and no database (mask, generalize, passthrough) |
| [`jobs/test/surrogate.yml`](jobs/test/surrogate.yml) | A safe-harbor profile with a `surrogate` field (stage 4) |
| [`jobs/test/deid-plain-test.json`](jobs/test/deid-plain-test.json) | A serverless wheel-task job for `plain.yml`, with no secrets |
| [`jobs/test/deid-surrogate-test.json`](jobs/test/deid-surrogate-test.json) | A serverless wheel-task job for `surrogate.yml`: hsm-core API v2 with its CA bundle, and Lakebase (stage 4) |
| [`jobs/test/deid-plain-dedicated-test.json`](jobs/test/deid-plain-dedicated-test.json), [`jobs/test/deid-surrogate-dedicated-test.json`](jobs/test/deid-surrogate-dedicated-test.json) | The same two jobs on a dedicated job cluster (one worker); only the compute differs |

### Stage 0: build and check the wheel

Optionally, run the full suite in Docker first:

```bash
cd hsm-databricks-udf && tests/deid/spark/run-tests.sh
```

Then build and check the wheel exactly as in section 4 (the `rm -rf`, `python -m
build --wheel`, then the check). Go on only when the check prints `wheel OK`.
To see the contents yourself:

```bash
unzip -l dist/hsm_databricks_udf-0.1.0-py3-none-any.whl | grep -E "dist-info/entry_points.txt|deid/restricted-zip3.txt"
unzip -p dist/hsm_databricks_udf-0.1.0-py3-none-any.whl 'hsm_databricks_udf-0.1.0.dist-info/entry_points.txt'
```

The first lists both files; the second prints `[console_scripts]` and
`hsm-deid = hsm_databricks_udf.deid.cli:main`.

### Stage 1: the test schema, volumes and table

Run [`jobs/test/giftcards.sql`](jobs/test/giftcards.sql) in a SQL editor or
notebook, then upload the wheel and the profiles:

```bash
databricks fs cp dist/hsm_databricks_udf-0.1.0-py3-none-any.whl dbfs:/Volumes/<catalog>/deid_test/libs/
databricks fs cp jobs/test/plain.yml dbfs:/Volumes/<catalog>/deid_test/deid/profiles/plain.yml
databricks fs cp jobs/test/surrogate.yml dbfs:/Volumes/<catalog>/deid_test/deid/profiles/surrogate.yml
```

### Stage 2: a notebook run (fastest feedback)

Attach a notebook to **serverless** first; repeat the stage on a **dedicated**
cluster afterwards. `cli.run` takes the same arguments as the job and returns
the exit status.

```python
# cell 1
%pip install /Volumes/<catalog>/deid_test/libs/hsm_databricks_udf-0.1.0-py3-none-any.whl "psycopg[binary]==3.2.3"
dbutils.library.restartPython()
```

```python
# cell 2: dry run (writes nothing, prints the review JSON)
from hsm_databricks_udf.deid import cli
args = ["--profile", "/Volumes/<catalog>/deid_test/deid/profiles/plain.yml",
        "--source-table", "<catalog>.deid_test.giftcards", "--key-column", "id",
        "--output-root", "/Volumes/<catalog>/deid_test/deid_out/plain"]
cli.run(args + ["--dry-run", "--sample", "4"])   # 0
```

Expect `0`, a review with `sample_rows: 4`, the method per field,
`dropped_by_default` listing `id`, `member_id`, `dob` and `status`, and nothing
in `deid_out/plain`.

```python
# cell 3: the real run
cli.run(args)   # 0
display(dbutils.fs.ls("/Volumes/<catalog>/deid_test/deid_out/plain"))
```

```python
# cell 4: check the CSV and the manifest
import glob, hashlib, json
out = "/Volumes/<catalog>/deid_test/deid_out/plain"
csv = glob.glob(out + "/*.csv")[0]
print(open(csv, encoding="utf-8").read())
m = json.load(open(glob.glob(out + "/*.manifest.json")[0]))
print(json.dumps(m, indent=2))
assert hashlib.sha256(open(csv, "rb").read()).hexdigest() == m["output"]["sha256"]
assert not glob.glob(out + "/*.part"), "the rename didn't happen"
assert not glob.glob(out + "/_deid_tmp/*"), "the temporary folder wasn't removed"
```

The rows, in any order (the same as the local test `test_job.py`):

| first_name | zip | amount | issued_date |
|---|---|---|---|
| `J***` | `606` | `25.00` | `2026-11-02T10:15` |
| `R**` | `000` | `100.00` | `2026-10-18T08:00` |
| `J***` | `606` | `'-5.00` | `2026-11-05T12:00:00.250` |
| | | `10.00` | |

What the rows check:
- **zip3 suppression:** `036` is a restricted zip3, so `03601` becomes `000`.
- **Formula-injection guard:** `-5.00` is written as `'-5.00`.
- **UTC session time zone:** the timestamps are written as stored, not shifted.

This stage covers most of section 9: `mapInPandas`, Parquet and text writes to
a volume, reading the output back on the driver, the session time zone and the
rename.

### Stage 3: the same profile as a wheel-task job

Tests the entry point and the job's environment, still without secrets.

```bash
databricks jobs create --json @jobs/test/deid-plain-test.json     # after replacing <catalog>
databricks jobs run-now <job_id> --python-params '["--profile","/Volumes/<catalog>/deid_test/deid/profiles/plain.yml","--source-table","<catalog>.deid_test.giftcards","--key-column","id","--output-root","/Volumes/<catalog>/deid_test/deid_out/plain","--dry-run","--sample","4"]'
databricks jobs get-run-output <task run id>
```

- The output shows the review JSON. Then run it without `--python-params` (the
  real run). The driver log has `deid_start` and a `deid_export_completed`
  audit event; the folder has a second CSV (the file name has `{run}` in it).
- **"Entry point not found":** set `package_name` to `hsm-databricks-udf`.
- **`environment_version` rejected:** use one your workspace lists.
- **Dedicated:** [`jobs/test/deid-plain-dedicated-test.json`](jobs/test/deid-plain-dedicated-test.json),
  the same parameters on a `SINGLE_USER` job cluster. Set `single_user_name`
  (the run-as service principal) and check `spark_version` and `node_type_id`
  against your workspace, then create and run it the same way. The parameters
  are the same, so a failure here and not on serverless is the cluster's.

### Stage 4: hsm-core and the surrogate table

1. Register the `app_id`, keypair and token (section 2) and put them, with
   hsm-core's CA bundle, in the secret scope (sections 3 and 3.1).
2. Serverless with network policies: allow the hsm-core and database hosts
   (section 6).
3. Create the surrogate-table key: cell 3 of the notebook below, or
   [`jobs/deid-init-key.json`](jobs/deid-init-key.json) (`STATIC`) or
   [`jobs/deid-init-key-jwt.json`](jobs/deid-init-key-jwt.json) (`SELF_SIGNED_JWT`) with
   `--name deid-test-surrogate`. Keep the `edek_id` it prints. This is the first
   call to hsm-core, so it proves the secrets, the CA bundle, V2 and the auth
   within seconds; it needs the `dek_issue` scope and prints `"reused": true` if
   the key already exists.
4. Create the surrogate table and the `deid_export` role in Lakebase or
   Postgres, and put the role's password in the scope (section 5).
5. Run `surrogate.yml`: first with `--dry-run`, then for real. Either from the
   notebook below (cells 4 and 5), or as the job
   [`jobs/test/deid-surrogate-test.json`](jobs/test/deid-surrogate-test.json)
   (replace every `<…>`; `--surrogate-key-edek-id` is the one from step 3; for
   Postgres, `--surrogate-db postgres` and its host and database). On a
   dedicated cluster, the same with
   [`jobs/test/deid-surrogate-dedicated-test.json`](jobs/test/deid-surrogate-dedicated-test.json):

   ```bash
   databricks jobs create --json @jobs/test/deid-surrogate-test.json
   databricks jobs run-now <job_id>
   databricks jobs get-run-output <task run id>
   ```

   `--python-params` replaces the whole parameter list, so for the job's dry run
   either pass the full list from the file with `"--dry-run","--sample","100"`
   added, or do the dry run from the notebook. Check:
   - rows 1 and 3 (both `M1234567`) have the same `member_ref`, row 4 another;
   - `SELECT domain, count(*) FROM deid_surrogate WHERE scope = 'test-vendor' GROUP BY 1`
     gives `member | 3` (one row per distinct member);
   - `dob` holds birth years and `zip` zip3s.
6. **Run it again.** Every `member_ref` must be the same as in the first file,
   and the table must still have 3 rows: members are reused, never given a new
   surrogate.

The notebook for this stage: the same parameters as the job, secret key names
only (the values stay in the scope). For a real vendor profile, change `JOB`.

```python
# cell 1: install
%pip install /Volumes/<catalog>/deid_test/libs/hsm_databricks_udf-0.1.0-py3-none-any.whl "psycopg[binary]==3.2.3"
dbutils.library.restartPython()
```

```python
# cell 2: the parameters
from hsm_databricks_udf.deid import cli

HSM = ["--secret-scope", "hsm",
       "--hsm-base-url", "https://<hsm-core-service host>/api/sensec/hsm/v2",
       "--hsm-api-version", "V2",
       "--hsm-app-id", "giftcard-deid-databricks",
       "--hsm-auth-mode", "STATIC",                  # SELF_SIGNED_JWT: and --hsm-signing-key-key instead of --hsm-token-key
       "--hsm-private-key-key", "deid-private-key",
       "--hsm-token-key", "deid-token",
       "--hsm-ca-cert-key", "deid-ca-bundle"]

DB = ["--surrogate-key-edek-id", "<edek_id from cell 3>",
      "--surrogate-db", "lakebase",                  # or postgres
      "--surrogate-db-host", "<instance host>",
      "--surrogate-db-name", "databricks_postgres",
      "--surrogate-db-user", "deid_export",
      "--surrogate-db-password-key", "deid-surrogate-db-password"]

OUT = "/Volumes/<catalog>/deid_test/deid_out/surrogate"
JOB = ["--profile", "/Volumes/<catalog>/deid_test/deid/profiles/surrogate.yml",
       "--source-table", "<catalog>.deid_test.giftcards",
       "--key-column", "id",
       "--output-root", OUT,
       "--triggered-by", "notebook:" + spark.sql("SELECT current_user()").first()[0]]
```

```python
# cell 3: the first call to hsm-core (TLS, V2, auth); creates the key or reuses it
cli.run(["--init-key", "--name", "deid-test-surrogate"] + HSM)   # 0, and prints {"edek_id": …}
```

```python
# cell 4: dry run (writes nothing, prints the review)
cli.run(JOB + HSM + DB + ["--dry-run", "--sample", "100"])        # 0
```

```python
# cell 5: the real run
rc = cli.run(JOB + HSM + DB)
print("exit status", rc)                                         # 0; 1 = failed (reason above), 2 = a wrong parameter
display(dbutils.fs.ls(OUT))
```

```python
# cell 6: check the newest CSV and its manifest
import glob, hashlib, json, os
csv = max(glob.glob(OUT + "/*.csv"), key=os.path.getmtime)
manifest = json.load(open(csv[:-4] + ".manifest.json"))
print(open(csv, encoding="utf-8").read())
print(json.dumps(manifest, indent=2))
assert hashlib.sha256(open(csv, "rb").read()).hexdigest() == manifest["output"]["sha256"]
assert not glob.glob(OUT + "/*.part") and not glob.glob(OUT + "/_deid_tmp/*")
```

Run cell 5 twice for step 6, and compare the two newest files.

### Stage 5 (optional): an encrypted column

Add a column encrypted with the package's `hsm_encrypt` function and give this
job's `app_id` a decrypt grant on its DEK (section 2, step 4). Then add a field
for it with `decrypt: true, method: surrogate, domain: card, normalize: digits`.

- Without `decrypt: true`, the dry run lists the column under
  `ciphertext_not_marked_decrypt`.
- With it, the run completes and the column holds surrogates.
- Without the grant, the run fails before writing anything and names the
  `edek_id`.

### Failures to try (from the stage 2 notebook)

| Do | Expect |
|---|---|
| Set `output.file` to `x_{yyyyMMdd}.csv` and run twice | The second run returns 1: "already exists; not overwriting it" |
| Add a field the table doesn't have | Returns 1, with a `deid_profile_problem` in the log |
| Comment out `hipaa-basis` | Returns 1: the profile fails validation |
| After each failed run | No `.csv`, no `.csv.part` and no `_deid_tmp/<run id>` folder |

### Clean up

```sql
DROP SCHEMA <catalog>.deid_test CASCADE;    -- the table, volumes and files
DELETE FROM deid_surrogate WHERE scope = 'test-vendor';   -- only in a table used for testing
```

Delete the test jobs, and the `deid-test-surrogate` key's grants if you made
any.
