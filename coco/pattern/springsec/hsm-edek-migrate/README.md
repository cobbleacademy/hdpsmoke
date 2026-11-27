# hsm-edek-migrate

Moves hsm-core's EDEKs from one tenant (A) to another (B) by re-wrapping each DEK:
unwrap with A's KEK, wrap with B's. Every DEK, `edek_id`, owner `app_id` and token
stays the same. Design, runbook and the reasons behind each choice:
[`../docs/EDEK_TENANT_MIGRATION.md`](../docs/EDEK_TENANT_MIGRATION.md).

**Tested** against real Postgres 16 with hsm-core's own Flyway migrations and
stand-in HSMs with real RSA-OAEP-256 keys (`EdekMigrateContainerTest`). **Not yet
run** against Key Vault or Managed HSM.

## Build

```bash
cd java
mvn -pl hsm-edek-migrate -am package
```

The jar is `hsm-edek-migrate/target/hsm-edek-migrate.jar`; it needs Java 21 or later.
Copy that one file to the machine that runs it.

## Where it runs

Any machine (VM, desktop or laptop) with Java 21 or later that can reach:

- both Postgres servers (5432);
- both vault URLs (443);
- `login.microsoftonline.com`, for both tenants.

If the vaults or databases are behind private endpoints or firewalls, that means
a VPN or peering, or allowing the machine's IP. The tool never calls hsm-core.

## Settings

A properties file. Print one with every key explained, then fill in the `<...>`
values:

```bash
java -jar hsm-edek-migrate.jar template > migrate.properties
```

The same file is [`src/main/resources/migrate.properties.template`](src/main/resources/migrate.properties.template).

Secrets aren't in it: each `*-env` key names the environment variable that holds
the value.

```bash
export EDEK_SOURCE_DB_PASSWORD='...'
export EDEK_TARGET_DB_PASSWORD='...'
export EDEK_SOURCE_CLIENT_SECRET='...'
export EDEK_TARGET_CLIENT_SECRET='...'
```

**Sign-in.** Each side needs its own service principal, since A's and B's vaults
are in different tenants: one in tenant A (`source.*`) and one in tenant B
(`target.*`). Off Azure, use `client-secret` or `client-certificate`
(`*.client-certificate-path`, a PEM with the key). `managed-identity` works only
on an Azure-hosted machine and `workload-identity` only in a Kubernetes pod;
`default` is the Azure SDK's chain. Postgres uses a user and password on each
side, not the service principal.

**KEK map.** One `kek-map.<A's KEK>=<B's KEK>` line per KEK name used in A,
including `source.legacy-kek-name`. `analyze` lists A's KEKs and reports any
without an entry. Map one to one: two A KEKs may point at one B KEK, but that
merges them for access, rotation and disabling (design section 11).

Every problem in the file is reported at once, and the tool exits with 2.

## Access

| Where | Needs |
|---|---|
| HSM-A | **unwrapKey** on A's KEKs, for the service principal in tenant A |
| HSM-B | **wrapKey** and **unwrapKey** on B's KEKs, for the service principal in tenant B |
| postgres-A | `SELECT` on `edek_records` (the tool's connection is read-only) |
| postgres-B | Create the staging schema and table (or have them created); read and write them; `SELECT`, `INSERT`, `UPDATE` on `edek_records`; `SELECT` on `app_registrations` |

Remove all of it after the move.

## Commands

```bash
java -jar hsm-edek-migrate.jar <command> --config migrate.properties [--kek <source KEK>] [--app <app_id>]
```

| Command | Does |
|---|---|
| `analyze [--no-probe]` | Counts A's rows per KEK, version, status and owner; checks the KEK map, owners in B, and clashes in B; probes both HSMs (`--no-probe` skips that). Writes nothing |
| `stage` | Brings the staging table in line with A (new rows, changed rows, rows gone from A). No HSM call. Run it again for the cutover pass |
| `wrap` | Unwraps each `staged` or `failed` row with A and wraps it with B |
| `verify` | Unwraps each `wrapped` row with both HSMs and compares the DEKs |
| `report` | Counts per source KEK, owner and status; the failed rows; whether the scope is ready to promote |
| `promote` | Copies `verified` rows into B's `edek_records` in one transaction; refused while any row in scope isn't verified |
| `reset --yes` | Deletes the staging rows in scope |
| `template` | Prints a settings file with every key explained |
| `help` | Prints the commands and exit codes |

`--kek` and `--app` limit any command to one source KEK and/or one owner.

| Exit | Means |
|---|---|
| 0 | Done |
| 1 | Stopped by an error, or by too many failures in a row; rerun continues where it stopped |
| 2 | A usage or settings error |
| 3 | Finished, with failed rows, problems found, or a refused promote |

## A run, one KEK at a time

```bash
java -jar hsm-edek-migrate.jar analyze --config m.properties
java -jar hsm-edek-migrate.jar stage   --config m.properties --kek hsm-master-kek
java -jar hsm-edek-migrate.jar wrap    --config m.properties --kek hsm-master-kek
java -jar hsm-edek-migrate.jar verify  --config m.properties --kek hsm-master-kek
java -jar hsm-edek-migrate.jar report  --config m.properties --kek hsm-master-kek
java -jar hsm-edek-migrate.jar promote --config m.properties --kek hsm-master-kek
```

Then restart B's hsm-core pods and decrypt real sample tokens through it. The
cutover pass is the same commands again (`stage` picks up new and changed rows).
The full order, with the rotation jobs turned off first, is section 8 of the design.

## What it never does

- Store, log or print a DEK, or anything derived from one. A DEK lives in memory
  for one row and is zeroed afterwards. A failed row records only the error's type.
- Change an `app_id`: it's the AES-GCM AAD (design section 6).
- Write to A.
