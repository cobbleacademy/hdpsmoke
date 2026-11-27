# Moving the EDEKs to another tenant (re-wrap design)

**Status: built, not yet run against real HSMs.** The tool is `java/hsm-edek-migrate`
(its [`README.md`](../hsm-edek-migrate/README.md) has the settings and commands). It's
tested against real Postgres with hsm-core's own migrations and stand-in HSMs
(`EdekMigrateContainerTest`), not against Key Vault or Managed HSM.

## 1. The scenario

| | A (running, temporary tenant) | B (new, permanent tenant) |
|---|---|---|
| KEK store | HSM-AKV-A (Key Vault or Managed HSM) | HSM-AKV-B |
| Database | postgres-A | postgres-B |
| App IDs | the same in both | the same in both |

The goal is for B to decrypt everything A encrypted, with the same tokens, and
to clean up on the way.

## 2. Why re-wrap

An EDEK is a DEK wrapped by a KEK (RSA-OAEP-256, `AzureKeyVaultKekClient`). The
ciphertext tokens hold only the `edek_id` (token layout: version, `edek_id`, IV,
tag, ciphertext), never the KEK.

| Approach | Verdict |
|---|---|
| Move the KEK from HSM-AKV-A to HSM-AKV-B | **Not possible.** HSM keys can't leave their tenant; Managed HSM backup and restore works only within one security domain and region (`DISASTER_RECOVERY.md`, Tier 0) |
| Decrypt all data in A, encrypt it again in B | Works, but rewrites every table and file that holds ciphertext, and every token changes |
| **Re-wrap: unwrap each DEK with A's KEK, wrap it with B's KEK** | **Chosen.** The DEKs, the `edek_id`s and every token stay the same; only `edek_blob`, `kek_name` and `kek_version` change |

The existing `POST /admin/rekey-kek` does this re-wrap (`RotationService.rekeyRecord`),
but only between keys of the one vault hsm-core is configured with
(`AZURE_KEYVAULT_URL`), so it can't be used between tenants. It also moves
only rows with `rotation_status` `current`, while this move needs every row
(section 3).

## 3. What each `edek_records` column does in the move

| Column | In B | Why |
|---|---|---|
| `edek_id` | **The same** | Every token names it |
| `app_id` (the DEK's owner) | **The same** | It's the AES-GCM AAD: under another value the data can't be decrypted |
| `edek_blob` | **New**: wrapped by B's KEK | The only crypto change |
| `kek_name` | **New**: B's KEK name, always set | A row with `kek_name` NULL in A means A's legacy default (`AZURE_KEK_NAME`); the tool resolves it to that name to unwrap, and writes B's name, never NULL |
| `kek_version` | **New**: the version B's wrap used | |
| `previous_edek_blob`, `previous_kek_name`, `previous_kek_version` | **Cleared** (NULL) | The rekey undo buffer; it points to A's KEK, which B can't use |
| `algorithm`, `encoding`, `fingerprint`, `data_classification`, `dek_name`, `current_dek_name`, `rotation_status`, `created_at`, `rotated_at` | **The same** | `fingerprint` comes from the ciphertext's IV and tag, not from the environment |

**Rows with `rotation_status` `rotated` must move too.** Named-DEK rotation
creates a new DEK for new data, but older ciphertext still names the old
`edek_id`, and `DekUnwrapService` looks a row up by ID whatever its status.
Dropping a `rotated` row makes that older data unreadable.

## 4. Other data

| Data | Move | How |
|---|---|---|
| `app_registrations` | Yes, with cleanup | Insert the rows in B (`ADMIN_OPERATIONS.md`, "A new environment"), then set each app's keys with `POST /admin/apps/keys`. Drop apps that are no longer used, but not an app that owns EDEKs that move; a replaced app_id stays, inactive (section 6) |
| `app_grants`, `app_dek_grants`, `app_decrypt_grants`, `app_classification_grants` | Yes, with cleanup | Copied by the tool, or as reviewed SQL; drop grants for apps that are dropped |
| `kek_registry` | Yes | `kek_name` changed from A's names to B's (the same name map as for the EDEKs) |
| Redis DEK cache | No | B starts with an empty cache |
| CEK secrets (`cek-*` in Key Vault Secrets) | No | B creates its own (`hsm-cache-key-rotator`); they only protect the Redis cache |
| Flyway history | No | B's hsm-core creates B's schema with its own migrations before the move |
| Audit logs | Not by the tool | Keep A's logs as long as your retention policy says |
| Ciphertext held by apps (tables, files) | Not by the tool | Unchanged; it only has to be reachable by the apps in B |

## 5. The tool: stage, verify, report, promote

A separate one-off batch tool, `java/hsm-edek-migrate`, run by hand from any VM, desktop or laptop that can reach both databases, both vaults and `login.microsoftonline.com`. It isn't part of hsm-core-service and isn't a live API, because:

- the running service would otherwise hold a credential for another tenant's
  HSM for good, for a job that runs once;
- it runs for a long time, and the move code shouldn't ship in the service image.

It reuses the same crypto: the Azure SDK `CryptographyClient`, RSA-OAEP-256, and an
unwrap pinned to the row's `kek_name` and `kek_version`, as
`AzureKeyVaultKekClient.unwrapDek` does.

The rows never go straight into B's `edek_records`. They're built in a staging
table in **postgres-B**, checked there, reported on, and only then promoted.
Nothing B serves changes until the promote, and the promote calls no HSM.

### 5.1 The staging table

In its own schema in postgres-B (for example `edek_migrate`), not in A: the
re-wrapped blobs belong to tenant B, and the promote is then a copy within one
database.

`edek_migrate.edek_records` has every column of `edek_records`, filled as in
section 3, plus:

| Column | For |
|---|---|
| `migrate_status` | `staged` → `wrapped` → `verified` → `promoted`; or `failed`; or `removed` (gone from A since it was staged) |
| `source_kek_name`, `source_kek_version` | The A KEK the row came from (`kek_name` NULL resolved to A's legacy default); the per-KEK report |
| `error_type` | Why a row failed: the error's type only, never a value |
| `source_row_hash` | A hash of the row's non-key columns in A, so a later pass finds rows that changed |
| `source_edek_blob` | A's EDEK (wrapped by A's KEK), so `wrap` and `verify` don't read A's database again |
| `batch_id`, `seen_batch`, `staged_at`, `wrapped_at`, `verified_at`, `promoted_at` | Which run did it, and when; `seen_batch` finds rows gone from A |

### 5.2 Inputs

| Input | Example |
|---|---|
| Source database and schema | postgres-A, `CRYPTO_SCHEMA`, `ACCESS_SCHEMA` |
| Target database, schemas | postgres-B: the staging schema, and B's `CRYPTO_SCHEMA` for the promote |
| Source vault URL and credential | HSM-AKV-A, a service principal in tenant A |
| Target vault URL and credential | HSM-AKV-B, a service principal in tenant B (a managed identity only if the tool runs on an Azure-hosted machine) |
| A's legacy default KEK name | A's `AZURE_KEK_NAME`, for rows with `kek_name` NULL |
| KEK name map | `hsm-master-kek` → `<B's KEK>`; every KEK name used in A must be mapped, or the tool stops before it starts |

There is **no app_id map** for `edek_records`: `app_id` is copied as is, always
(section 6).

The settings file, with every key explained, is printed by `template`; see
[`../hsm-edek-migrate/README.md`](../hsm-edek-migrate/README.md).

### 5.3 Commands

| Command | Does |
|---|---|
| `analyze` | Reads A, checks B, writes nothing. Counts rows per `kek_name`, `kek_version`, `rotation_status` and `app_id`; checks the KEK name map covers every name, every owner app_id has a row in B's `app_registrations`, and B's `edek_records` doesn't already hold any of A's `edek_id`s (with another owner or fingerprint) or `current_dek_name`s. Then probes both HSMs: one unwrap per A KEK version (with a real row), one wrap and unwrap of random bytes per B KEK. `--no-probe` skips the HSMs |
| `stage` | Brings staging in line with A, with no HSM call: new rows added as `staged`; rows whose `source_row_hash` changed (or that came back) reset to `staged`; rows gone from A marked `removed`. Safe to run any number of times: the first run, and the cutover pass, are the same command. Refuses while a KEK in A has no `kek-map` entry |
| `wrap` | For each `staged` or `failed` row: unwrap with HSM-A, wrap with HSM-B, store the new `edek_blob` and `kek_version`, set `wrapped` |
| `verify` | For each `wrapped` row: unwrap the new blob with HSM-B, and again the A blob with HSM-A; compare the two DEKs in memory; set `verified`, or `failed` |
| `report` | Per KEK (and per `app_id`): counts by `migrate_status`, and the failed `edek_id`s with `error_type` |
| `promote` | Copies `verified` rows into B's `edek_records` in one transaction (section 5.5) |
| `reset --yes` | Empties the staging table (or one KEK's or app's rows of it) |
| `template` | Prints a settings file with every key explained (`template > migrate.properties`) |

Every command takes `--kek <source KEK>` and `--app <app_id>`, so each KEK can be
staged, run, checked, reported on and promoted on its own before the next.

Exit status: 0 done; 1 stopped by an error (rerun continues); 2 a usage or settings
error; 3 finished, but with failed rows, problems found, or a refused promote.

### 5.4 Each row in `wrap` and `verify`

1. Unwrap A's `edek_blob` with HSM-A (`source_kek_name`, `source_kek_version`).
2. `wrap`: wrap the DEK with HSM-B (B's KEK from the map); keep the version it
   returns. `verify`: unwrap the staged blob with HSM-B and compare it with the
   DEK from step 1.
3. Zero the DEK's bytes. It exists only in memory: never stored (not even a
   hash of it), never logged, never in an error message.

Every row is verified, not a sample: it's two more HSM calls per row, and a
sample leaves rows unchecked. Sample only if the row count is very large next
to the HSM's throughput limit, and say so in the report.

A failed row is marked `failed` with its `error_type`, and the run goes on.
`max-consecutive-failures` failures in a row (10 by default: an HSM or a database
is down) end the run early, as `RotationService` does. `wrap` reruns `staged` and
`failed` rows; a row that fails `verify` goes back through `wrap`.

### 5.5 Promote

```sql
-- one transaction, refused while any row of the chosen group isn't 'verified'
BEGIN;
-- 0. rows promoted earlier give up a current_dek_name they no longer hold
UPDATE <B crypto schema>.edek_records t SET current_dek_name = NULL FROM edek_migrate.edek_records s
WHERE s.edek_id = t.edek_id AND s.migrate_status = 'verified'
  AND t.current_dek_name IS DISTINCT FROM s.current_dek_name;
-- 1. rows promoted earlier that changed in A (the cutover pass)
UPDATE <B crypto schema>.edek_records t SET <every column except edek_id> = <s's value>
FROM edek_migrate.edek_records s
WHERE s.edek_id = t.edek_id AND s.migrate_status = 'verified';
-- 2. new rows
INSERT INTO <B crypto schema>.edek_records (<the edek_records columns>)
SELECT <the same columns> FROM edek_migrate.edek_records s
WHERE s.migrate_status = 'verified'
  AND NOT EXISTS (SELECT 1 FROM <B crypto schema>.edek_records t WHERE t.edek_id = s.edek_id);
COMMIT;
```

- Updates go first, after step 0: `current_dek_name` is unique across all rows
  (V14), and Postgres checks it row by row, so a row that stopped being current
  must give up its name before the row that took it over gets it. (With the rotation jobs off, step 1 of the runbook, this
  should never happen, but the order makes it safe if it does.)
- The tool refuses to promote while the group has `staged`, `wrapped`, `failed`
  or `removed` rows, unless you promote a smaller group (`--kek`, `--app`) that's
  fully verified. It also refuses a verified row whose `edek_id` is already in B's
  table but wasn't put there by an earlier promote: that would overwrite
  another row.
- After the promote, restart B's hsm-core pods, then decrypt real sample tokens
  from every app through B's normal API. hsm-core stores no ciphertext, so this
  is the only end-to-end test of the data itself.

**Rollback before callers move:** delete the promoted rows from B's
`edek_records` (or empty it, as B had none), fix, run again.

## 6. A dead app_id replaced by a new one

**`edek_records.app_id` is never changed.** The owner app_id is the AES-GCM AAD of
every ciphertext made with that DEK (`EncryptionService`, the `ResolvedDek`
javadoc). Under another app_id every tag check fails, and that data can never be
decrypted again. Re-wrapping changes only the KEK, so it can't fix that.

A replacement is a change to the access tables only:

1. Keep the old app's `app_registrations` row, with `active` false, so it can't
   sign in. The grant check (`AppRegistryService.isGranted`) doesn't require the
   owner to be active.
2. Give the new app_id `decrypt` on the old owner's DEKs: a coarse grant
   (`app_grants`) for all of them, or per `dek_name` (`app_dek_grants`).
3. If the new app keeps writing under the same `dek_name`s, give it `encrypt` too.
   A cross-app reuse keeps encrypting with the old owner's DEK and AAD. `dek_name`s
   are reserved across all apps (V14), so the new app couldn't create its own DEK
   under those names anyway.
4. New `dek_name`s the new app creates are owned by the new app_id.

| Option | Data rewritten | Code change | Verdict |
|---|---|---|---|
| **The old app_id stays the owner; the new app gets grants** | None | None | **Use this** |
| Decrypt as the old app_id, encrypt again as the new one | All of that app's data | None in hsm-core | Only if the old app_id must disappear completely (for example, an audit requirement) |
| Separate the AAD app_id from the owner in hsm-core (a new column; `/dek/unwrap` returns the AAD app_id) | None | Yes, and it changes the client contract (`COMPATIBILITY_CONTRACT.md`) | Not worth it |

`analyze` lists the owner app_ids of A's rows. Any of them marked as replaced
must appear in B as an inactive row with the grants above before the promote.

## 7. Access and network

| Who | Needs | When |
|---|---|---|
| The tool, in tenant A | **unwrapKey only**, on A's KEKs only (for Managed HSM, a key-scoped *Managed HSM Crypto User* role assignment) | Removed after the move |
| The tool, in tenant B | **wrapKey** and **unwrapKey** (for `verify`) on B's KEK | Removed after the move; B's hsm-core keeps its own access |
| The tool, databases | `SELECT` on postgres-A; the staging schema in postgres-B; `INSERT`/`UPDATE` on B's `edek_records` for the promote | Removed after the move |
| Network | Where the tool runs must reach both HSMs and both databases (private endpoints or firewall rules on each) | |

A managed identity works only in its own tenant, so at least one of the two
credentials is a service principal. Keep its secret or certificate in a key
vault, not in the tool's settings.

## 8. Runbook

| Step | What | Where |
|---|---|---|
| 0 | B's hsm-core deployed with B's KEK (`AZURE_KEYVAULT_URL`, `AZURE_KEK_NAME`), Flyway done, no callers yet; the staging schema created | B |
| 1 | `KEK_ROTATION_ENABLED=false` and `NAMED_DEK_ROTATION_ENABLED=false` on hsm-core-service **and** hsm-bulk-service, in A and in B; check no rotation or rekey is running (`RUNBOOK.md`, "Rotation jobs") | A, B |
| 2 | Copy the access tables, with the cleanups and app_id replacements (sections 4, 6, 9) | B |
| 3 | `analyze` | Tool |
| 4 | Per KEK: `stage`, `wrap`, `verify`, `report`; fix and rerun until the KEK has no `failed` rows. A keeps serving | Tool |
| 5 | `promote`; restart B's pods; decrypt real sample tokens of every app through B | B |
| 6 | **Cutover window:** stop new DEK creation in A (deactivate the callers with `POST /admin/apps/status`, or stop their traffic); `stage` (it picks up new and changed rows), `wrap`, `verify`, `report`, `promote`; the sample tokens again | A, tool, B |
| 7 | Move the callers to B; turn B's rotation jobs back on | B |
| 8 | Keep A read-only until a set date; then drop the staging schema, remove the tool's access in both tenants, and decommission A | A, B |

## 9. Cleanups

| Safe to drop | Not safe to drop |
|---|---|
| Apps and grants no longer used, if no EDEK that moves is owned by them | `rotated` EDEK rows (section 3) |
| `previous_*` columns (cleared anyway) | Any EDEK that ciphertext still names, unless that data is deleted too |
| `kek_registry` entries for apps that are dropped | A replaced app's row: keep it, inactive (section 6) |

Dropping EDEKs on purpose (data that's no longer kept) is a data-deletion
decision for its owner, made on the ciphertext and its EDEK together, not by
this tool.

## 10. Risks

| Risk | Mitigation |
|---|---|
| A DEK leaks during the move | Memory only, zeroed after each row; nothing about it stored or logged; a locked-down host; credentials removed afterwards |
| A wrong blob reaches B | Staging: every row is verified before `promote`, and `promote` refuses unverified rows |
| Rows change in A during the move | Rotation jobs off (step 1); `stage` again in the cutover window, with `source_row_hash` |
| A KEK name in A has no B mapping | `analyze` stops before anything is staged |
| `edek_id` or `current_dek_name` clashes in B | `analyze` checks; `promote` is one transaction |
| An app_id renamed in `edek_records` | Not possible: the tool has no app_id map; replacements go through grants (section 6) |
| A partial run | Per-row status in staging; rerun picks up `staged` and `failed` rows |

## 11. Decisions still open

1. Where the tool runs, so it can reach both tenants' HSMs and databases.
2. One KEK in B, or one per KEK in A (the name map allows either).
3. The cutover window's length, and how callers are stopped in A during it.
4. Which apps and grants the cleanup drops, and which app_ids are replaced.
5. How long A stays read-only after the move.
6. Whether the HSMs' throughput allows verifying every row in the time
   available, or the report has to say a sample was used.
