# Disaster Recovery: Failover, Backup, and Restore

What needs protecting, in priority order, what mechanism protects it, and
what's genuinely safe to not back up at all. Where a target (RTO/RPO,
retention window) depends on your organization's actual compliance/business
requirements, it's marked `TODO` — the structure below is fixed; the numbers
aren't mine to set.

## Component tiers

| Tier | Component | Why | Backup mechanism |
|---|---|---|---|
| **0 — must not be lost** | KEK (Azure Managed HSM) | Losing it makes every EDEK in existence permanently unrecoverable | Azure Managed HSM native backup/restore |
| **0** | EDEK store (`hsm_crypto.edek_records`, Postgres) | The wrapped DEK bytes live here — lose this and the KEK alone can't recover the data, there's nothing left to unwrap | Postgres point-in-time restore + cross-region replica |
| **0** | Access store (`hsm_access`: `app_registrations`, `app_grants`, `app_dek_grants`) | Doesn't lose data, but losing it *is* the total-lockout scenario in `RUNBOOK.md` | Same DB, same treatment as EDEK store |
| **1 — recoverable, restore promptly** | CEK secrets (`cek-alpha`/`cek-beta`/`cek-current-key`, Key Vault Secrets) | Losing these breaks the Redis cache layer only — direct HSM unwrap still works | Key Vault soft-delete + purge protection |
| **1** | Config / deployment (Helm values, K8s secrets, `application.yml`) | Should already be in version control / a secrets manager | Standard GitOps hygiene, not a new mechanism |
| **2 — fully disposable** | Redis DEK cache | TTL'd cache; `RedisDekCache`/`NullDekCache` already tolerate it being empty or entirely absent | **None needed** — do not spend effort backing this up |

## Tier 0: KEK (Azure Managed HSM)

This is Azure's mechanism, not this codebase's — the job here is to confirm
it's actually configured and tested for *your* HSM instance, not assume it
is because Azure offers it.

```bash
# Backup (produces an encrypted blob, restorable only into another Managed
# HSM in the same Azure region and security domain)
az keyvault backup start --hsm-name <hsm-name> --blob-storage-name <storage-account> --blob-container-name <container>

# Restore
az keyvault restore start --hsm-name <target-hsm-name> --blob-storage-name <storage-account> --blob-container-name <container> --folder-to-restore <backup-folder>
```

- [ ] `TODO`: confirm backup is actually scheduled (not just possible) for
      the production HSM instance
- [ ] `TODO`: schedule an annual restore rehearsal into a scratch HSM —
      an untested backup is not a backup
- [ ] `TODO`: confirm the security-domain/region constraint on restore
      matches your actual DR region plan (a backup can't be restored
      cross-region or cross-security-domain)

## Tier 0: EDEK store + Access store (Postgres)

The single most consequential backup target in the whole system —
prioritize this over everything else here.

- If using Azure Database for PostgreSQL: automated backups + point-in-time
  restore are largely built-in. Verify the **retention window** matches
  your actual requirement — `TODO`: confirm current retention (Azure's
  default may be shorter than what you need).
- For cross-region DR: configure a read replica or geo-restore target.
  `TODO`: decide whether cross-region DR is a requirement at all for this
  service, and if so, target region.
- If self-managed Postgres: `pg_dump` on a schedule is a floor, not a
  target — prefer continuous WAL archiving for real point-in-time restore
  rather than only-as-fresh-as-last-dump backups.

**Restore drill checklist** (run this at least as often as the HSM drill
above, ideally more often given this is the higher-consequence target):

- [ ] Restore a recent backup into a scratch environment
- [ ] Confirm `edek_records` row counts and a sample decrypt round-trip
      succeed against the restored data (using a non-production KEK/EDEK
      pair — never exercise this against production keys)
- [ ] Confirm `app_registrations`/`app_grants`/`app_dek_grants` restored
      correctly and match expected state
- [ ] Time the restore — this is your actual RTO for this component, not
      whatever number is in a document until it's been measured

## Tier 1: CEK secrets (Key Vault Secrets)

```bash
az keyvault secret backup --vault-name <vault-name> --name cek-alpha --file cek-alpha.backup
az keyvault secret restore --vault-name <vault-name> --file cek-alpha.backup
```

- [ ] Enable soft-delete + purge protection on the Key Vault holding these
      (cheap, should already be on for any production Key Vault, worth
      confirming explicitly for this one)
- If lost entirely with no backup: not a data-loss event. Set
  `DEK_CACHE_ENABLED=false` to disable the Redis layer, redeploy, and
  reprovision fresh CEK secrets (see `scripts/provision_dek_cache_key.py`
  or its ported equivalent) at leisure — direct HSM unwrap continues
  serving every decrypt in the meantime, just without the cache's latency
  benefit (see `CACHING_AND_ROTATION.md` for what that costs under load).

## Tier 2: Redis DEK cache — explicitly not backed up

State this in writing so nobody spends effort on it: Redis here is a
disposable TTL cache. A cold cache after any restore/failover just means
the first decrypt of each `edek_id` pays the HSM-unwrap cost again — there
is no data at risk and no backup mechanism to build.

## Failover / fallback summary

| Failure | Fallback behavior | Manual action needed? |
|---|---|---|
| Redis unreachable | Falls through to direct HSM unwrap automatically | No |
| CEK secrets lost | Same as above, once `DEK_CACHE_ENABLED=false` is set | Yes — one config flag + redeploy |
| CEK rotation service down | Pods hold current CEK indefinitely, no errors | No (see `RUNBOOK.md`) |
| Postgres (EDEK/access store) lost | **No fallback — this is the actual disaster scenario** | Yes — full restore, see drill above |
| Managed HSM lost | **No fallback — every EDEK becomes unrecoverable without it** | Yes — full restore into another HSM |
| JWKS/IdP outage | No fallback today (see `RUNBOOK.md`'s total-lockout section) | Yes, or wait it out |

The pattern worth internalizing: everything in Tier 1/2 already degrades
gracefully in code (Redis and CEK-secret loss are non-events). Everything in
Tier 0 has **zero** in-code fallback — losing the HSM or the Postgres store
is an actual disaster requiring the restore procedures above, not something
the running service can route around on its own. Plan and drill
accordingly: don't let Tier 1's graceful degradation create false
confidence about Tier 0.
