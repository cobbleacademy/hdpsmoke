# Audit & Scanning Tool Boundary

What an external scanning/compliance tool can and cannot see when it touches
this service's KEK and EDEK data, and how that differs from the in-service
`governance` scope.

## Two separate mechanisms, two separate postures

### 1. External read-only scanning (the "Auditor SPN" in the architecture diagram)

An audit/inventory/compliance scanner is a **separate identity, outside this
service's API entirely**:

| It can read | It cannot do |
|---|---|
| KEK metadata via Azure KV (`az keyvault key list-versions`) — version IDs, creation dates, rotation policy | Export or use the KEK — Managed HSM has no key-export path, full stop |
| `edek_records` directly (`hsm_crypto` schema) — `edek_blob`, owner `app_id`, `kek_version`, algorithm, classification | Unwrap `edek_blob` — no wrap/unwrap RBAC on the HSM is granted to this identity |
| `hsm_access` schema — who has which scopes, which grants exist | Reach the Redis DEK cache, or route through `/decrypt` at all |

`edek_blob` is base64 ciphertext (the DEK, wrapped by the KEK). Reading the
row is useless without HSM unwrap rights, which this identity deliberately
never has. This is why a raw DB dump, a disk-level scan, or a DB
backup/export landing somewhere unexpected only ever turns up opaque
ciphertext tokens — not a leak of the underlying data. It's also why the
scanning identity is architecturally *incapable* of the thing it audits: a
segregation-of-duties property, not just a policy statement.

**Recommendation if this identity doesn't exist yet as a distinct Azure
principal:** provision one explicitly (its own service principal / managed
identity), grant it `Key Vault Reader`-equivalent (list/get metadata, no
crypto operations) on the HSM and read-only DB roles on `hsm_crypto` +
`hsm_access`, and keep it fully separate from the "Service SPN" the running
pods use. Mixing the two identities defeats the separation.

### 2. In-service `governance` scope (`DecryptionService`)

A completely different mechanism, for a completely different need: a caller
holding the `governance` scope can decrypt data it doesn't own **without a
grant** — the per-record cross-app grant check
(`AppRegistryService.isGranted`) is skipped, but everything else is not:
valid JWT, active `app_id`, the `decrypt` scope itself, and full normal audit
logging (`app_id`, `sub`, `edek_id`, `owner_app_id`, `caller_ip`) still apply.

Use this for a legitimate internal audit workflow that genuinely needs
plaintext (e.g., a compliance investigation), where the trail of *who
decrypted what, when* matters. Never use it as a substitute for the
read-only scanner above — `governance` should be assigned to as few
identities as possible, since holding it is functionally "can read anyone's
data."

## Summary

| | External scanner | `governance` scope |
|---|---|---|
| Can decrypt? | Never | Yes, deliberately |
| Bypasses | Nothing — no crypto access at all | Only the cross-app grant check |
| Audit trail | Azure KV diagnostic logs + DB audit (separate pipeline) | This service's own `audit_log`, same as any decrypt |
| Assign to | A dedicated read-only identity, held by few/no humans directly | As few app registrations as possible |
