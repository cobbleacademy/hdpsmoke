# KEK Lifecycle: Expiry, Renewal and Monitoring (Azure Key Vault / Managed HSM)

How KEK expiry works in Key Vault, what hsm-core does when a KEK expires, the
setup that keeps it from happening, what hsm-core monitors, and what to do if
a KEK has already expired.

Short version:

- **Expiry never breaks decrypt.** Key Vault still allows unwrap on an expired
  version.
- **Expiry stops new wraps,** and so new encrypts, DEK issue, KEK rotation and
  rekey into that KEK.
- **Disabling or deleting a version is what breaks decrypt.**
- Key Vault owns version creation, through a rotation policy. hsm-core
  re-wraps records onto the new version, and watches expiry.

## 1. How Key Vault treats expiry

Expiry (`exp`) and not-before (`nbf`) belong to each key **version**. The key
as a whole has no expiry.

| Version state | wrapKey / encrypt | unwrapKey / decrypt | get (metadata) |
|---|---|---|---|
| Valid | yes | yes | yes |
| **Expired** (past `exp`) | **no, 403** | **yes** | yes |
| Not yet valid (before `nbf`) | no, 403 | yes | yes |
| **Disabled** (`enabled=false`) | no | **no** | yes |
| Deleted (soft-deleted) | no | no | no (recoverable while the vault retains deleted keys) |

Key Vault keeps unwrap working on expired versions on purpose: data protected
while the key was valid stays recoverable.

## 2. How hsm-core uses KEK versions

- **Wrap** (encrypt, DEK issue, rotation, rekey, named-DEK rotation) calls the
  **versionless** key id, so Key Vault uses the **latest** version at that
  moment. The version that was actually used comes back in the result and is
  stored on the row (`edek_records.kek_version`). A new Key Vault version is
  picked up without a restart (commit `129f7e1`).
- **Unwrap** (decrypt, DEK unwrap) calls the **exact version stored on the
  row.**
- **KEK rotation** (`RotationService.rotateKek`, cron `KEK_ROTATION_CRON` or
  `POST /admin/rotate-kek`) re-wraps every `CURRENT` row onto the latest
  version. It does **not** create versions; Key Vault does that.
- **Rows that are never re-wrapped:**
  - Rows left `ROTATED` by named-DEK rotation keep their original KEK version
    permanently.
  - Decrypting old ciphertext that points at those rows still needs that
    version.

Consequence: **only the latest version's expiry matters for availability.**
Old versions only unwrap, which expiry doesn't block.

## 3. What happens if a KEK's latest version expires

| Function | Behaviour |
|---|---|
| Encrypt needing a new DEK, `/dek/issue` | **Fails.** Key Vault returns 403 and the request errors. |
| Decrypt, `/dek/unwrap` | **Keeps working.** |
| KEK rotation, that KEK | **Skipped before any row is touched.** Logs `kek_rotation_target_unusable`. `POST /admin/rotate-kek?kekName=<kek>` returns **409**. A full sweep returns 200 with `status=skipped` for that KEK and rotates the others. |
| Rekey into that KEK | **409** before any row is touched (`kek_rekey_target_unusable`). |
| Named-DEK rotation for rows on that KEK | Each row fails and is retried next run. The sweep aborts after 10 consecutive failures. |
| `/admin/health` | **`status=degraded`, `kek_ok=false`.** Before this change it said `ok`. |
| Daily expiry check | Logs at **ERROR**: `kek_expiry_check … state=expired`. |

## 4. Prevent it: Key Vault setup (no code)

### 4.1 Rotation policy on every KEK

The policy creates a new version before the current one expires, so the latest
version never expires while the policy runs.

`policy.json`:

```json
{
  "lifetimeActions": [
    { "trigger": { "timeBeforeExpiry": "P60D" }, "action": { "type": "Rotate" } },
    { "trigger": { "timeBeforeExpiry": "P30D" }, "action": { "type": "Notify" } }
  ],
  "attributes": { "expiryTime": "P1Y" }
}
```

```bash
az keyvault key rotation-policy update --vault-name <vault> --name <kek> --value policy.json
az keyvault key rotation-policy show   --vault-name <vault> --name <kek>
```

- Each new version expires 1 year after creation and is replaced 60 days
  before that. Set the period your security standard requires.
- `expiryTime` applies only to versions created **after** the policy is set.
  Give the **current** version an expiry too, or rotate once immediately:

  ```bash
  az keyvault key set-attributes --vault-name <vault> --name <kek> --expires 2027-11-01T00:00:00Z
  # or
  az keyvault key rotate --vault-name <vault> --name <kek>
  ```

- Managed HSM: use `--hsm-name <hsm>` instead of `--vault-name`. Rotation
  policies are supported there too.

### 4.2 Move records onto new versions promptly

- Run `KEK_ROTATION_CRON` daily or weekly, not the chart default of monthly
  (`0 2 1 * *`). A run with nothing to re-wrap costs one Key Vault GET per KEK.
- After a policy rotation, records move within a day or a week.

### 4.3 Old versions

- **Leave expired old versions alone.** They are harmless.
- **Disable an old version only after nothing references it,** `ROTATED` rows
  included:

  ```sql
  SELECT kek_name, kek_version, rotation_status, count(*)
  FROM hsm_crypto.edek_records
  GROUP BY 1, 2, 3
  ORDER BY 1, 2;
  ```

- **Never delete KEK versions.** Enable **soft delete and purge protection**
  on the vault or HSM so a mistaken delete can be recovered.

### 4.4 Permissions

| Identity | Role | Can |
|---|---|---|
| hsm-core workload identity | **Key Vault Crypto User** (Managed HSM: *Managed HSM Crypto User*) | get, wrapKey, unwrapKey |
| Key operators (people / pipeline) | **Key Vault Crypto Officer** | create versions, set policy and attributes |
| Nobody routinely | purge | — |

The app cannot disable, delete or purge keys.

### 4.5 Alerts and guardrails in Azure

- **Event Grid** on the vault: `Microsoft.KeyVault.KeyNearExpiry` and
  `Microsoft.KeyVault.KeyExpired`. The policy's `Notify` action raises the
  near-expiry event. Route both to an action group (email, pager).
- **Azure Policy**, audit or deny:
  - *Keys should have an expiration date*
  - *Keys should have a rotation policy ensuring that their rotation is
    scheduled within the specified number of days after creation*
  - *Keys should have more than the specified number of days before
    expiration*
- **Diagnostic logs** to Log Analytics: alert on `WrapKey` with result 403.

## 5. What hsm-core monitors

`KekExpiryMonitor` checks the **latest version** of every KEK in use: the
legacy default (`hsm.azure.kek-name`), every `kek_name` in `kek_registry`, and
every `kek_name` on a current `edek_records` row. Each check is one Key Vault
GET. The app needs only the *get* permission, which Crypto User already has.

| State | Meaning | Wraps? |
|---|---|---|
| `ok` | Expiry beyond the warning window | yes |
| `no_expiry` | Version has no expiry set (allowed, not recommended) | yes |
| `expiring` | Fewer than `KEK_EXPIRY_WARN_DAYS` days left | yes |
| `expired` | Past expiry | **no** |
| `not_yet_valid` | Before not-before | **no** |
| `disabled` | `enabled=false` (decrypt also fails for rows on this version) | **no** |
| `unknown` | Lookup failed: key deleted, access removed, vault unreachable | — |

### 5.1 `/admin/health`

```json
{"status":"ok","vault_reachable":true,"db_reachable":true,
 "kek_ok":true,"kek_expires_at":"2027-10-01T00:00:00Z","kek_days_left":320}
```

- `kek_ok=false` when any KEK in use is `expired`, `disabled`,
  `not_yet_valid` or `unknown`. `status` is then `degraded`.
- `kek_expires_at` and `kek_days_left` describe the KEK that expires first.
  Both are null when no KEK has an expiry.
- KEK names are not shown, because the endpoint is public. The daily log line
  names the KEK.
- The result is cached for `KEK_EXPIRY_CACHE_TTL_SECONDS` (default 900), so
  probes don't call Key Vault for every KEK. A fix (a new version) shows in
  health within that time.
- **HTTP status is still 200 even when degraded.** If health is later changed
  to return 503 on degraded, keep KEK state out of the **liveness** probe:
  restarting pods does not fix an expired key.

### 5.2 Daily `kek_expiry_check` log

The check runs about 30 seconds after startup and then on
`KEK_EXPIRY_CHECK_CRON` (default `0 6 * * *`). It runs on every pod of a
release with `KEK_EXPIRY_CHECK_ENABLED=true`. The hsm-core chart has it on;
the hsm-bulk chart has it off to avoid duplicate alerts. It is not run in demo
mode.

```text
INFO  kek_expiry_check kek_name=kek-pay version=3f… state=ok expires_on=2027-10-01T00:00Z days_left=320 enabled=true not_before=null
WARN  kek_expiry_check kek_name=kek-hr  version=9a… state=expiring expires_on=2026-12-01T00:00Z days_left=15 …
ERROR kek_expiry_check kek_name=kek-old version=11… state=expired  expires_on=2026-11-01T00:00Z days_left=-14 …
WARN  kek_expiry_check_failed kek_name=kek-x error=ResourceNotFoundException: …
INFO  kek_expiry_check_completed keks=4 ok=1 expiring=1 failing=2 warn_days=30
```

Suggested Splunk alerts:

- `kek_expiry_check state=expiring` gives a ticket.
- `kek_expiry_check (state=expired OR state=disabled OR state=not_yet_valid)`
  OR `kek_expiry_check_failed` pages someone.
- `kek_rotation_target_unusable` OR `kek_rotation_job_skipped_keks` pages
  someone.

### 5.3 Rotation pre-check

Before re-wrapping, rotation and rekey read the target KEK's latest version.
If it is `expired`, `disabled` or `not_yet_valid`:

- They log `kek_rotation_target_unusable` / `kek_rekey_target_unusable` at
  ERROR, with the version, the reason, `expires_on` and `not_before`.
- They write an audit event of the same name.
- They touch no row.
- A single-KEK rotation or a rekey returns **409**. A full sweep skips that
  KEK (`status=skipped`, `skip_reason=…` in the response) and the scheduler
  logs `kek_rotation_job_skipped_keks`.

### 5.4 Settings

| Env var | Default | Chart value |
|---|---|---|
| `KEK_EXPIRY_CHECK_CRON` | `0 6 * * *` | `config.kekExpiryCheckCron` |
| `KEK_EXPIRY_CHECK_ENABLED` | `true` | `config.kekExpiryCheckEnabled` (bulk: `false`) |
| `KEK_EXPIRY_WARN_DAYS` | `30` | `config.kekExpiryWarnDays` |
| `KEK_EXPIRY_CACHE_TTL_SECONDS` | `900` | — (env only) |

Set `KEK_EXPIRY_WARN_DAYS` **shorter** than the policy's
`Rotate timeBeforeExpiry` (60 days above). If `expiring` then ever fires, it
means the policy did not rotate.

### 5.5 Checking it's deployed

A build without this feature returns only `status`, `vault_reachable` and
`db_reachable` from `/admin/health`.

| Where | Check | Deployed |
|---|---|---|
| Running pod | `curl -s https://<host>/api/sensec/hsm/v1/admin/health` | Response includes `kek_ok`, `kek_expires_at`, `kek_days_left` |
| hsm-core pod logs (`DEMO_MODE=false`) | `kubectl -n <ns> logs <pod> \| grep kek_expiry` | `kek_expiry_check_scheduler_started` at startup, then about 30s later one `kek_expiry_check` line per KEK and `kek_expiry_check_completed` |
| Pod env | `kubectl -n <ns> get configmap <release> -o yaml \| grep KEK_EXPIRY` | `KEK_EXPIRY_CHECK_ENABLED=true` (hsm-core) / `false` (hsm-bulk) |
| Image (distroless, so no shell in the pod) | `docker create --name c <image> && docker cp c:/app/app.jar . && docker rm c`, then `unzip -l app.jar \| grep -E "KekStatus\|KekExpiryMonitor\|KekExpiryCheckScheduler"` | All three classes listed |
| Source copied to the build side | `grep -n 86_400L java/hsm-core-service/src/main/java/com/hsm/core/crypto/KekStatus.java` | One match. This is the final version; an older copy has `toHours(), 24L`. |

**Health has `kek_ok` but there is no log line.** Either the pod is
hsm-bulk, where the check is off by design, or `DEMO_MODE=true`.

**The log line is there but no `kek_expiry_check` lines follow.** Every
scheduled job shares one thread by default. A long KEK rotation run delays the
startup check until it finishes. Look for `kek_expiry_check_job_failed` or
`kek_expiry_check_failed` too.

## 6. Emergency procedure: a KEK has expired (or is about to)

Symptoms:

- Encrypts fail with a 5xx while decrypts work.
- `kek_ok=false` in health.
- `kek_expiry_check state=expired`.
- `kek_rotation_target_unusable`.

Steps:

1. **Create a new version** (or push the current version's expiry out):

   ```bash
   az keyvault key rotate --vault-name <vault> --name <kek>
   # or, short term:
   az keyvault key set-attributes --vault-name <vault> --name <kek> --expires <new-date>
   ```

2. **Encrypts recover immediately.** Wraps use the latest version and no
   restart is needed.
3. **Re-wrap existing records:**

   ```bash
   curl -X POST "https://<host>/api/sensec/hsm/v2/admin/rotate-kek?kekName=<kek>" \
     -H "Authorization: Bearer <rotate-scope token>" -H "X-App-ID: <ops app>"
   ```

4. Health shows `kek_ok=true` within `KEK_EXPIRY_CACHE_TTL_SECONDS`.
5. **Find out why the policy didn't rotate:**
   - Is the policy missing (`rotation-policy show`)?
   - Did the policy start after the version was created?
   - Is a Crypto Officer role missing for the policy's operations?

**If a version was disabled by mistake:** re-enable it with
`az keyvault key set-attributes … --version <v> --enabled true`. Decrypts of
rows on that version fail until then.

**If a key was deleted:** recover it with `az keyvault key recover`. This
works while soft delete retains it, which is why purge protection matters.

## 7. Checklist per KEK

- [ ] Rotation policy set (`Rotate` before expiry, `Notify`, `expiryTime`)
- [ ] Current version has an expiry
- [ ] Soft delete + purge protection on the vault / HSM
- [ ] App identity is Crypto User only
- [ ] Event Grid `KeyNearExpiry` / `KeyExpired` routed to on-call
- [ ] `KEK_ROTATION_CRON` daily or weekly
- [ ] Splunk alerts on `kek_expiry_check` / `kek_rotation_target_unusable`
- [ ] Old versions disabled only after the SQL in 4.3 shows zero rows
