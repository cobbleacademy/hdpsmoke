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
java -jar hsm-edek-migrate.jar <command> --config migrate.properties [--kek <source KEK>[,...]] [--app <app_id>[,...]] [--ids <file>]
```

| Command | Does |
|---|---|
| `analyze [--no-probe]` | Counts A's rows per KEK, version, status and owner; checks the KEK map, owners in B, and clashes in B; probes both HSMs (`--no-probe` skips that). Writes nothing |
| `stage` | Brings the staging table in line with A (new rows, changed rows, rows gone from A). No HSM call. Run it again for the cutover pass |
| `wrap` | Unwraps each `staged` or `failed` row with A and wraps it with B |
| `verify` | Unwraps each `wrapped` row with both HSMs and compares the DEKs |
| `report` | Counts per source KEK, owner and status; the failed rows; whether the scope is ready to promote; with `--ids`, each listed row's status |
| `promote` | Copies `verified` rows into B's `edek_records` in one transaction; refused while any row in scope isn't verified |
| `reset --yes` | Deletes the staging rows in scope |
| `template` | Prints a settings file with every key explained |
| `help` | Prints the commands and exit codes |

Limits, for any command; a row must match each one given:

| Option | Limits to |
|---|---|
| `--kek a,b` | These source KEKs (a NULL `kek_name` is `source.legacy-kek-name`). Repeat it to add more |
| `--app x,y` | These owner `app_id`s. Repeat it to add more |
| `--ids <file>` | These `edek_id`s: one per line, blank lines and `#` comments skipped |

| Exit | Means |
|---|---|
| 0 | Done |
| 1 | Stopped by an error, or by too many failures in a row; rerun continues where it stopped |
| 2 | A usage or settings error |
| 3 | Finished, with failed rows, problems found, or a refused promote |

## When a command stops

The error names the side, the step and the server's reason, then says what to check:

```
can't connect to the source database pg-a-host:5432/hsm as edek_migrate_reader: password authentication failed for user "edek_migrate_reader" (SQLState 28P01)
  check: the server was reached and rejected the user or password: ...
analyze stopped: target database, checking owners in hsm_access.app_registrations: permission denied for schema hsm_access (SQLState 42501)
  check: target.user is missing a grant this step needs: ...
```

| SQLState | Means | Usually |
|---|---|---|
| `08001` | This machine can't reach the server | Host or port wrong, the server's firewall rules, VPN, private-endpoint DNS. `nc -vz <host> 5432` from the same machine. `No suitable driver` instead: the `jdbc-url` isn't `jdbc:postgresql://...` |
| `08004` | The server refused the connection | `sslmode`, or the server doesn't accept this machine |
| `28P01` | Wrong user or password | The user, or the password in the `*-password-env` variable: export it in single quotes so the shell leaves `$`, `!` and backticks alone |
| `28000` | User or machine not allowed | The user doesn't exist on this server, or password sign-in is off |
| `3D000` | No such database | The database name in `jdbc-url` |
| `42501` | Permission denied | A grant from [Access](#access) is missing |
| `42P01` / `3F000` | No such table or schema | `*.crypto-schema`, `target.access-schema`; hsm-core's migrations must have run on B |

Vault errors show the Entra or Key Vault reason, for example `AADSTS7000215` (wrong client
secret) or `403 ... ForbiddenByRbac` (missing role on the KEK). `wrap` and `verify` print
it once per error type; the staging row keeps only the type.

## A run, one KEK at a time

```bash
java -jar hsm-edek-migrate.jar analyze --config m.properties
java -jar hsm-edek-migrate.jar stage   --config m.properties --kek hsm-master-kek
java -jar hsm-edek-migrate.jar wrap    --config m.properties --kek hsm-master-kek
java -jar hsm-edek-migrate.jar verify  --config m.properties --kek hsm-master-kek
java -jar hsm-edek-migrate.jar report  --config m.properties --kek hsm-master-kek
java -jar hsm-edek-migrate.jar promote --config m.properties --kek hsm-master-kek
```

Several KEKs at once: `--kek hsm-master-kek,kek-pii`.

Then restart B's hsm-core pods and decrypt real sample tokens through it. The
cutover pass is the same commands again (`stage` picks up new and changed rows).
The full order, with the rotation jobs turned off first, is section 8 of the design.

## A trial promote of chosen rows

```bash
printf '%s\n' 3f2b...-... 9c41...-... > sample.txt   # edek_ids, one per line
java -jar hsm-edek-migrate.jar analyze --config m.properties --ids sample.txt   # lists ids not in A
java -jar hsm-edek-migrate.jar stage   --config m.properties --ids sample.txt
java -jar hsm-edek-migrate.jar wrap    --config m.properties --ids sample.txt
java -jar hsm-edek-migrate.jar verify  --config m.properties --ids sample.txt
java -jar hsm-edek-migrate.jar report  --config m.properties --ids sample.txt   # each row's status
java -jar hsm-edek-migrate.jar promote --config m.properties --ids sample.txt
```

Only the listed rows go into B's `edek_records`, and only they block that promote.
Restart B's pods and decrypt the tokens of those rows through B.

B can decrypt only rows that were promoted: each `edek_id` is its own DEK. Promote
the rest (the same commands without `--ids`) before any caller moves to B. That
promote inserts the rest and leaves the sample alone.

## What it never does

- Store, log or print a DEK, or anything derived from one. A DEK lives in memory
  for one row and is zeroed afterwards. A failed row records only the error's type.
  Errors print the server's or vault's reason, never Postgres's "Detail" line (which
  can quote a row), and anything that looks like a password or token is redacted.
- Change an `app_id`: it's the AES-GCM AAD (design section 6).
- Write to A.
