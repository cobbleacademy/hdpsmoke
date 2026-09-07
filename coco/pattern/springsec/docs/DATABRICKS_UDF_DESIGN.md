# Databricks-Native Encrypt/Decrypt UDFs — Design

Status: **built and verified against a real hsm-core-service instance** —
see [`../../hsm-databricks-udf/`](../../hsm-databricks-udf/) (package),
[`../../hsm-databricks-udf/DEPLOYMENT.md`](../../hsm-databricks-udf/DEPLOYMENT.md)
(deployment steps per compute type + example queries). **The Databricks-side
deployment steps themselves have not been run against a real Databricks
workspace** — this repo has no Databricks access; only the crypto/protocol
layer is proven, twice over (a local self-consistency suite, and a real
cross-implementation round-trip against a live server in both directions).
Companion to [`SPARK_ADAPTER.md`](SPARK_ADAPTER.md), but a deliberately
different artifact, not an extension of it — see "Why not `hsm-spark-adapter`"
below.

**A real, serious bug in `hsm-core-service` itself was found and fixed while
building this** — see §6's "AAD" note and `EncryptionService.ResolvedDek`'s
javadoc. A grant-authorized cross-app `dek_name` reuse (the exact scenario
V14's grant model exists to support) produced a ciphertext nothing could ever
decrypt again, because the AES-GCM AAD used the *caller's* app_id rather than
the DEK's true, permanent owner. Fixed in `EncryptionService`, `DekIssueService`,
and `DekUnwrapService` (plus their response DTOs, which never exposed
`owner_app_id` at all on the `/dek/issue`/`/dek/unwrap` path — this package
needed that field, building it surfaced the gap). The equivalent bug still
exists in the JVM clients (`hsm-bulk-client`'s `DbBulkJob`/`FileBulkJob`,
`hsm-crypto-client`'s `HsmCryptoClient`) — confirmed present, not yet fixed;
flagged as a follow-up, out of scope for this design doc.

## 1. What this is, in one paragraph

A standalone Python package that registers `hsm_encrypt`/`hsm_decrypt` as
**Unity Catalog Python Functions**, callable from SQL, notebooks, Delta Live
Tables, and jobs across job clusters, Unity-Catalog shared clusters, *and*
serverless compute — the three surfaces the existing JVM `hsm-spark-adapter`
cannot uniformly reach. It talks to `hsm-core-service`'s existing
`POST /dek/issue` / `POST /dek/unwrap` endpoints (already built for
`hsm-bulk-client`, not new), and reuses this repo's existing Python AES-256-GCM
primitives (`app/crypto/dek_manager.py`, `app/crypto/iv_factory.py`) rather
than reimplementing them.

## 2. Why not just deploy `hsm-spark-adapter` onto Databricks

Checked directly against current Databricks documentation, not assumed:

- **Java/Scala UDFs are excluded from Unity Catalog shared clusters, by
  design** — they only run on single-user/no-isolation clusters. `hsm-spark-adapter`'s
  `HsmUdfExtension` is a Scala/JVM `SparkSessionExtensions` implementation, so
  it inherits this ceiling regardless of packaging.
- **`spark.sql.extensions` and all compute-scoped libraries are excluded from
  serverless entirely**, also by design. There is no configuration path that
  gets a JVM Spark extension running on serverless compute.
- **`HsmThriftServerBootstrap` doesn't apply to Databricks at all.** It exists
  to expose JDBC-servable decrypt views from a *self-managed* Spark cluster.
  Databricks already owns that serving layer via DBSQL warehouses — bootstrapping
  a second, embedded Thrift Server inside a Databricks cluster would duplicate
  infrastructure Databricks already provides.
- **Spark/Scala version skew.** `hsm-spark-adapter` is built against
  `spark-sql_2.13:4.2.0`; the latest confirmed Databricks Runtime (18.2, May
  2026) ships Spark 4.1.0 — a minor-version gap against a `provided` dependency,
  a real risk of `NoSuchMethodError`-class failures, not just a version-number
  footnote.

None of this is a packaging problem `hsm-spark-adapter` can be tweaked around.
The mechanism itself (JVM Spark extension) has a hard ceiling at job/classic
clusters. Reaching all three compute types requires a mechanism Databricks
hasn't fenced off anywhere: **Python (or SQL) Unity Catalog Functions**, which
are supported broadly, including outbound HTTPS calls from serverless (subject
to egress allowlisting — see §7).

## 3. Goals / non-goals

**Goals:**
- `hsm_encrypt(plaintext, dek_name, data_classification)` and
  `hsm_decrypt(ciphertext_token)` invocable identically from SQL, PySpark
  DataFrames, and Delta Live Tables, on job, shared, and serverless compute.
- Token-format compatible with `hsm-core-service`'s own `/encrypt`/`/decrypt` —
  a row encrypted via this UDF must decrypt through the ordinary `/decrypt`
  endpoint with zero awareness of how it was produced, matching the existing
  `hsm-bulk-client`/`FileBulkJob` interoperability guarantee.
- Throughput comparable to `HsmCryptoClient`'s model: one `/dek/issue` or
  `/dek/unwrap` call per DEK, not per row.

**Non-goals (this round):**
- Not porting `HsmSqlScriptRunner` or `HsmThriftServerBootstrap` — Databricks'
  own view-creation and serving mechanisms already cover that ground.
- Not building a general-purpose Databricks connector for arbitrary crypto
  operations — scoped to the existing `hsm_encrypt`/`hsm_decrypt` UDF surface
  `hsm-spark-adapter` already defines the contract for.
- Not solving FIPS-140 validation for the local per-row AES-GCM path —
  confirmed out of scope; the requirement holds for key generation and
  custody (the HSM boundary and the wrap step server-side), not for
  downstream local crypto operations. See §8.

## 4. Compute-surface coverage (target state)

| Compute type | UDF registration mechanism | Local AES-GCM crypto | Status |
|---|---|---|---|
| Job / classic all-purpose clusters | Unity Catalog Python Function, or plain PySpark UDF | Standard `cryptography` (no FIPS provider needed) | Reachable |
| Shared clusters (Unity Catalog standard access mode) | Unity Catalog Python Function | Same | Reachable, pending admin allowlist for the package/wheel (governance control, not a technical blocker) |
| Serverless (notebooks, jobs, SQL) | Unity Catalog Python Function | Same | Reachable, pending egress allowlist for `hsm-core-service`'s domain if the workspace runs restricted egress mode |

All three rows now use the *same* mechanism and the *same* crypto path — no
tiering by compute type is needed, unlike the earlier draft of this design
that assumed FIPS might force a split. That assumption is resolved (§8).

## 5. Code reuse — not a rewrite

Checked directly: `app/crypto/dek_manager.py` and `app/crypto/iv_factory.py`
depend on nothing but `cryptography` and the stdlib — no FastAPI, no
SQLAlchemy, no coupling to the rest of `app/`. They already implement, in
Python, byte-for-byte the same token format `hsm-core-service` produces:

```
pack_token(edek_id, iv, tag, ciphertext) -> "v1.<base64url(1B version | 16B edek_id | 12B iv | 16B tag | ciphertext)>"
unpack_token(token) -> UnpackedToken(edek_id, iv, tag, ciphertext)
```

and IV generation already uses `secrets.token_bytes()` — a real OS CSPRNG,
matching the rigor `BULK_OPERATIONS.md` already specifies for non-JVM clients
("the platform's real CSPRNG"). **Plan: vendor these two modules into the new
package rather than importing `app/crypto` directly** (avoids pulling in
`app/`'s full dependency tree, and decouples the new package's release cadence
from the FastAPI service's) — copied, not reimplemented, so there is zero risk
of the token format silently drifting from what `/decrypt` actually parses.

## 6. New package shape

Proposed as a new top-level directory, sibling to `java/` and
`spark-verification-app/` — **not** inside `app/`, since it needs to be
packaged and distributed independently (as a wheel, uploaded to a Unity
Catalog volume or an internal PyPI index) with a much smaller dependency
footprint than the FastAPI service:

```
hsm-databricks-udf/
  pyproject.toml
  src/hsm_databricks_udf/
    __init__.py
    dek_manager.py       # vendored from app/crypto/dek_manager.py
    iv_factory.py         # vendored from app/crypto/iv_factory.py
    transport.py          # RSA-OAEP-256 wrap/unwrap, Python side (new — see below)
    svc_client.py         # HTTP client for /dek/issue, /dek/unwrap (new)
    cache.py              # per-worker-process DEK cache (new)
    udf.py                # hsm_encrypt / hsm_decrypt entry points + CREATE FUNCTION DDL helper
  tests/
```

`transport.py` is the one genuinely new crypto-adjacent module. It's a direct
Python port of `TransportWrapper`'s exact transformation, reproducible with
the standard `cryptography` package (no FIPS provider required, since it's
confirmed out of the custody boundary — see §8):

```python
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes

def unwrap(wrapped_dek: bytes, private_key) -> bytes:
    return private_key.decrypt(
        wrapped_dek,
        padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA256()),
                     algorithm=hashes.SHA256(), label=None),
    )
```

This matches `TransportWrapper`'s Java transformation
(`RSA/ECB/OAEPWithSHA-256AndMGF1Padding`) exactly — same algorithm, same
padding scheme, same hash, interoperable ciphertext in both directions.

## 7. Wire protocol — reusing existing endpoints, not adding new ones

No changes to `hsm-core-service` are required. The package calls the same two
endpoints `hsm-bulk-client` already uses:

- **`POST /dek/issue`** `{key, data_classification, name}` → `{edek_id,
  wrapped_dek_b64, reused}` per item. First call for a given `dek_name` mints
  a DEK (subject to the V14 global-ownership/grant check, same as any other
  caller); subsequent calls for the same name reuse it.
- **`POST /dek/unwrap`** `{key, edek_id}` → `{wrapped_dek_b64}` per item, for
  decrypting rows whose `edek_id` is already known (e.g., re-processing
  previously-encrypted data).

Auth: reuses the existing `X-App-ID` + Bearer JWT model. The most natural fit
for a Databricks worker process is **`SELF_SIGNED_JWT`** (a local keypair, no
network round-trip to mint a token, matching `AUTHORIZATION.md` §1a) or
**mTLS** if the workspace's network config supports presenting a client
certificate to `hsm-core-service` — either avoids a dependency on Azure AD
token-issuance latency inside a tight per-partition UDF init path. The
service's private key/cert would live in a **Databricks secret scope**, read
once at worker-process startup, never per-row.

## 8. FIPS scope — resolved, one item still open

**Resolved:** the FIPS 140 requirement applies to key generation and custody
— the HSM boundary and BC-FIPS's approved-mode wrap, both server-side. It does
not extend to downstream local crypto performed by an authorized client. This
means:
- The repeated, per-row **AES-256-GCM** operation needs no FIPS-validated
  module. Standard `cryptography` (OpenSSL-backed) is sufficient, with no
  dependency on controlling the cluster's OpenSSL build — which is exactly
  what makes serverless viable for this (no init scripts, no custom images
  available there; a FIPS-provider requirement would have blocked it).

**Still open, low-stakes, worth a two-minute confirmation before build:** the
**RSA-OAEP transport-unwrap** (§6, `transport.py`) sits right on the "custody"
boundary — it's the step that turns HSM-wrapped key material into raw,
plaintext-usable bytes. Whether that specific operation counts as "custody"
(and so still needs a FIPS-validated module) or as "an authorized recipient
opening what it was handed" (and so doesn't) wasn't asked as a separate
question. The stakes are small either way: unwrap happens **once per DEK**,
not once per row, so even the strict answer doesn't force a FIPS-capable
runtime onto every compute surface — worst case, that one call routes through
a narrower, FIPS-aware path while the bulk per-row work stays exactly as
designed above.

## 9. Caching design — mirroring `HsmCryptoClient`, not reinventing it

PySpark reuses the same Python worker process across many rows within a
partition (standard execution model, not Unity-Catalog-specific). `cache.py`
exploits that directly:

```python
_dek_cache: dict[str, bytes] = {}   # module-level -- persists across UDF calls within one worker process

def get_dek(dek_name: str, svc_client, private_key) -> bytes:
    if dek_name not in _dek_cache:
        wrapped = svc_client.issue(dek_name)
        _dek_cache[dek_name] = transport.unwrap(wrapped, private_key)
    return _dek_cache[dek_name]
```

Same shape as `HsmCryptoClient`'s `encryptCacheByName`/`decryptCacheByEdekId`
— no TTL, unbounded for the worker process's lifetime, same tradeoff already
accepted and documented for the JVM client (see `AUTHORIZATION.md` §1c on
client-side DEK memory exposure — that analysis applies identically here and
isn't re-litigated by this design).

## 10. Network egress by compute type

- **Job/classic, shared clusters:** standard VPC/VNet egress configuration,
  no different from any other external HTTPS dependency these clusters
  already have.
- **Serverless:** Python UDFs can reach external endpoints on ports 80/443 —
  confirmed directly against Databricks' own documentation — but if the
  workspace runs *restricted* egress mode, `hsm-core-service`'s domain must be
  explicitly added to the allowed internet-domains list. This is a one-time
  workspace network-policy change, not a per-job configuration.

## 11. Registering the UDFs

Unity Catalog Python Functions are registered via `CREATE FUNCTION`, not
`spark.sql.extensions`:

```sql
CREATE OR REPLACE FUNCTION main.hsm.hsm_decrypt(ciphertext_token STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
    from hsm_databricks_udf.udf import decrypt
    return decrypt(ciphertext_token)
$$;
```

The function becomes a governed Unity Catalog object — grantable/revocable via
standard Unity Catalog permissions, auditable via Unity Catalog's own audit
log, independent of which cluster or warehouse a caller uses to invoke it.
This is a meaningful side benefit over the current Spark-extension model:
access to `hsm_decrypt` itself becomes a Unity Catalog grant, on top of
whatever `hsm-core-service`-side authorization already applies.

## 12. Error handling

Per-row failures (a malformed token, a `403` from a denied grant, a `dek_name`
owned by a different app with no grant) should surface as a UDF exception
that fails the *row*, not silently return null — matching how `/decrypt`
itself fails loudly rather than swallowing errors. A batched variant
(`hsm_decrypt_batch`, a Python UDTF) is worth considering in a follow-up round
if per-row Python UDF call overhead turns out to dominate — out of scope for
the initial design.

## 13. Testing/verification plan

1. **Token interop test, no Databricks involved:** encrypt via a real
   `hsm-core-service` `/encrypt` call, decrypt via this package's Python
   `dek_manager`; and the reverse — encrypt via the package, decrypt via
   `/decrypt`. Proves wire-format compatibility before any Databricks
   dependency enters the picture, same verification bar
   `CoreBulkFileInteropTest` already holds the JVM client to.
2. **Job cluster prototype:** register as a plain PySpark UDF (not yet a
   Unity Catalog Function) on a real job cluster, run against a real
   `hsm-core-service` instance, confirm throughput is comparable to
   `HsmCryptoClient`'s (DEK issued/unwrapped once, not per row).
3. **Shared cluster:** same test, registered as an actual `CREATE FUNCTION`
   Unity Catalog object, confirm the admin allowlist path works as expected.
4. **Serverless:** same test on serverless job/notebook compute, confirm
   egress reaches `hsm-core-service` under the workspace's actual network
   policy.

## 14. Open questions, explicitly

- RSA-OAEP unwrap's exact FIPS scope (§8) — small stakes, still worth a direct
  answer before build rather than assuming.
- Where does the compiled wheel get distributed — a Unity Catalog volume,
  an internal PyPI index, or bundled directly into the job's dependencies?
  Affects the shared-cluster admin-allowlist mechanics.
- Auth mode: `SELF_SIGNED_JWT` vs mTLS for the Databricks-side credential —
  depends on whether the target workspace's network setup can present a
  client certificate to `hsm-core-service` at all.
- Whether a Python UDTF (table-valued, batched) is worth building alongside
  the scalar UDF from the start, or only if per-row call overhead proves to
  matter in practice.
