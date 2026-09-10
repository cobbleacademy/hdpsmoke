# Databricks-Native Encrypt/Decrypt UDFs — Design

Status: **built and verified against a real hsm-core-service instance, and
confirmed working end-to-end against a real Databricks workspace** — both
`STATIC` and `SELF_SIGNED_JWT` auth modes. See
[`../../hsm-databricks-udf/`](../../hsm-databricks-udf/) (package),
[`../../hsm-databricks-udf/DEPLOYMENT.md`](../../hsm-databricks-udf/DEPLOYMENT.md)
(deployment steps + example queries, and the authoritative, currently-correct
version of everything below — this doc lags it slightly on fast-moving
detail). This repo itself has no Databricks access, so that live
verification happened on the deploying user's own workspace — treat
compute types, network policies, or code paths not explicitly exercised
there as still unconfirmed rather than assuming full coverage. Companion to
[`SPARK_ADAPTER.md`](SPARK_ADAPTER.md), but a deliberately different
artifact, not an extension of it — see "Why not `hsm-spark-adapter`" below.

**Correction #1, found on a real deployment attempt**: the original design
(§11) had the function body call `dbutils.secrets.get(...)` directly, which
does not work — `dbutils` is only available in the calling notebook/job's
own driver context, confirmed directly against Databricks' own docs/KB,
never inside a UDF or Unity Catalog Python Function body. `hsm_encrypt_detail`/
`hsm_decrypt_detail` now take an explicit `credentials_json` argument
instead, built via Databricks' native `secret(scope, key)` scalar
expression (a general SQL function, not confined to `CREATE CONNECTION`)
combined with `to_json(named_struct(...))` — pure SQL, no `dbutils`/
notebook/Python required at all.

**Correction #2, also found live**: the natural next step — a
`hsm_credentials()` helper function, called *from inside* the simplified
`hsm_encrypt`/`hsm_decrypt` wrappers' own bodies — reliably broke Unity
Catalog with `UC_INVALID_DEPENDENCIES.SQL_UDF`, confirmed not fixable by
restarting a cluster/warehouse or by dropping and recreating every function
in the chain. Isolated by direct experiment: a `LANGUAGE SQL` function's
body calling *another user-defined SQL function* (`hsm_decrypt` calling
`hsm_credentials()`) is what breaks; the same function calling a
`LANGUAGE PYTHON` function (`hsm_decrypt` calling `hsm_decrypt_detail`) is
reliable. `hsm_encrypt`/`hsm_decrypt` now **inline** the credentials
expression directly in their own body instead of calling a shared helper —
at the real cost of that expression existing in three separate places
(`hsm_credentials()`, kept standalone for ad-hoc use; `hsm_encrypt()`;
`hsm_decrypt()`) with no single source of truth. Because a plain
`LANGUAGE SQL` function still runs with its *owner's* privileges by default
(definer rights, same as a view), the governance goal holds regardless:
callers of `hsm_encrypt`/`hsm_decrypt` need only `EXECUTE`, never `READ
SECRET` on the underlying scope.

**Correction #3, also found live**: `secret(...)` — whether called
directly, via `hsm_credentials()`, or inlined in `hsm_encrypt`/
`hsm_decrypt` — **cannot be used anywhere feeding a persisted write**
(`CREATE TABLE ... AS SELECT`, `INSERT INTO ... SELECT`) at all. Databricks
blocks this categorically (`SECRET_FUNCTION_INVALID_LOCATION`), confirmed
against Databricks' own error-class docs, regardless of whether the
persisted output actually contains the secret value. Extracting
`credentials_json` a level up via `.collect()` doesn't work around it
either: Databricks redacts the *output* of any SQL command invoking
`secret()`, substituting the literal string `"[REDACTED]"` before the value
reaches the Python driver — confirmed live (a `credentials_json` built this
way came back with both key/token fields replaced by 10-character
`"[REDACTED]"` strings, not the real values). Since bulk table encrypt/
decrypt (the actual primary use case for this package) is exactly this
kind of persisted write, this is a significant finding, not an edge case.
The confirmed fix: resolve `credentials_json` via `dbutils.secrets.get(...)`
in the notebook/job's own driver code instead — never through SQL's
`secret()` for this path — and pass it to `hsm_encrypt_detail`/
`hsm_decrypt_detail` (never the simplified wrappers, which have `secret()`
inlined) as a genuine bound parameter. `dbutils.secrets.get()` runs in the
driver's own Python process, not as a SQL command invoking `secret()`, so
neither restriction applies to it. Confirmed working end-to-end: a real
`CREATE TABLE ... AS SELECT main.hsm.hsm_encrypt_detail(...)` using this
pattern succeeded. Net result: there are now **two genuinely distinct
calling patterns** depending on use case — the simplified wrappers for
ad-hoc reads, `_detail` + driver-side `dbutils.secrets.get()` for any
persisted write — not one universal pattern.

See `DEPLOYMENT.md` for the full mechanism, the current correct SQL, and
the decision table for which pattern to use.

**A real, serious bug in `hsm-core-service` itself was found and fixed while
building this** — see §6's "AAD" note and `EncryptionService.ResolvedDek`'s
javadoc. A grant-authorized cross-app `dek_name` reuse (the exact scenario
V14's grant model exists to support) produced a ciphertext nothing could ever
decrypt again, because the AES-GCM AAD used the *caller's* app_id rather than
the DEK's true, permanent owner. Fixed in `EncryptionService`, `DekIssueService`,
and `DekUnwrapService` (plus their response DTOs, which never exposed
`owner_app_id` at all on the `/dek/issue`/`/dek/unwrap` path — this package
needed that field, building it surfaced the gap). The equivalent bug also
existed in the JVM clients (`hsm-bulk-client`'s `DbBulkJob`/`FileBulkJob`,
`hsm-crypto-client`'s `HsmCryptoClient`) — since fixed, same pattern
(`ownerAppId` threaded through the cached DEK value and used as the AAD
instead of the client's own configured `appId`); confirmed via
`CoreBulkFileInteropTest` and a new `HsmCryptoClientTest`, full multi-module
suite green (125 tests). `hsm-spark-adapter` needed no change — its UDFs are
pure delegates to `HsmCryptoClient`, no independent AAD logic.

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
`spark.sql.extensions`. The function's own `ENVIRONMENT` clause declares its
dependencies (this package's wheel, uploaded to a Unity Catalog volume) —
resolved wherever the function actually executes, not wherever it was
registered from; confirmed directly against Databricks' `CREATE FUNCTION`
docs, not assumed.

**Credentials are NOT resolved via `dbutils.secrets.get(...)` inside the
function body — an earlier draft of this section said they were, and that
was wrong.** Confirmed directly against Databricks' own docs/KB once this
was actually exercised: `dbutils` is only available in the calling
notebook/job's own driver context, never inside a UDF or Unity Catalog
Python Function body. A `CREATE FUNCTION ... LANGUAGE PYTHON` body calling
`dbutils.secrets.get(...)` raises `NameError: name 'dbutils' is not defined`
— confirmed live, not merely from docs, after this design shipped and a
real deployment attempt hit exactly that.

Fixed by having the real crypto function, `hsm_decrypt_detail`, take an
explicit `credentials_json` argument, built in SQL via `secret(scope, key)`
(a general Databricks SQL scalar expression, Runtime 11.3 LTS+, not
confined to `CREATE CONNECTION`) combined with `to_json(named_struct(...))`
— no `dbutils`, notebook, or Python required anywhere in the calling path.

**The example below shows the design as it *should* work, but does not, in
practice** — see Correction #2 in the status header above and
`DEPLOYMENT.md`'s "How credentials reach the function" for the confirmed,
current fix (`hsm_encrypt`/`hsm_decrypt` inline the credentials expression
rather than calling `hsm_credentials()` from their own body). Left here to
show the intent; do not copy the `hsm_decrypt` definition below verbatim:

```sql
CREATE OR REPLACE FUNCTION main.hsm.hsm_credentials()
RETURNS STRING
LANGUAGE SQL
RETURN to_json(named_struct(
    'HSM_SERVICE_BASE_URL', 'https://hsm-core-service.internal:8443/api/sensec/hsm/v1',
    'HSM_APP_ID',           'databricks-udf',
    'HSM_AUTH_MODE',        'STATIC',
    'HSM_PRIVATE_KEY_PEM',  secret('hsm', 'databricks-udf-private-key'),
    'HSM_BEARER_TOKEN',     secret('hsm', 'databricks-udf-token')
));

CREATE OR REPLACE FUNCTION main.hsm.hsm_decrypt_detail(
    ciphertext_token STRING,
    credentials_json STRING
)
RETURNS STRING
LANGUAGE PYTHON
ENVIRONMENT (
  dependencies = '["cryptography>=42.0", "requests>=2.31",
                   "/Volumes/main/hsm/libs/hsm_databricks_udf-0.1.0-py3-none-any.whl"]',
  environment_version = 'None'
)
AS $$
    from hsm_databricks_udf.udf import decrypt
    return decrypt(ciphertext_token, credentials_json)
$$;

-- BROKEN as written -- confirmed live: a LANGUAGE SQL function's body
-- calling another user-defined SQL function (hsm_credentials() here)
-- corrupts Unity Catalog's dependency tracking, raising
-- UC_INVALID_DEPENDENCIES.SQL_UDF on every subsequent call. See
-- DEPLOYMENT.md for the actual, working version (the credentials
-- expression inlined directly, not this call):
CREATE OR REPLACE FUNCTION main.hsm.hsm_decrypt(ciphertext_token STRING)
RETURNS STRING
LANGUAGE SQL
RETURN main.hsm.hsm_decrypt_detail(ciphertext_token, main.hsm.hsm_credentials());
```

called as, from pure SQL — a SQL editor, a dashboard, a SQL warehouse query,
or `spark.sql(...)` in a notebook, all identical:

```sql
SELECT main.hsm.hsm_decrypt(ciphertext_token) FROM t;
```

`hsm_decrypt_detail` (with an explicit `credentials_json`) stays available
for a caller needing a non-default credential — calling `hsm_credentials()`
from an *ad-hoc* query alongside it is fine; it's only calling it from
*inside another function's own body* that breaks. Unity Catalog doesn't
support function overloading — `CREATE OR REPLACE` requires the same
parameter list as before, confirmed directly against Databricks' own docs
— so the simplified and explicit forms need distinct names, not two arities
of one `hsm_decrypt`. See `../../hsm-databricks-udf/sql/create_functions.sql`
and `../../hsm-databricks-udf/DEPLOYMENT.md` for the full, current, working
version.

The functions are still governed Unity Catalog objects — grantable/revocable
via standard Unity Catalog permissions, auditable via Unity Catalog's own
audit log, independent of which cluster or warehouse a caller uses to invoke
them. That side benefit over the Spark-extension model is unchanged.
**Governance is still preserved** with the inlined fix: a plain
`CREATE FUNCTION ... LANGUAGE SQL` body runs with its *owner's* privileges
by default (definer rights, same as a view — confirmed directly against
Databricks' docs), so `secret(...)` inside `hsm_encrypt`/`hsm_decrypt`'s
inlined expression checks the owner's `READ SECRET` grant, never the
caller's. Only whoever registers these functions needs `READ SECRET` on the
`hsm` scope; a caller of `hsm_encrypt`/`hsm_decrypt` needs only `EXECUTE`
and never touches the raw secret at all.

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

- ~~Whether `secret(...)` can be used inside a function that also feeds a
  persisted write (`CREATE TABLE ... AS SELECT`/`INSERT INTO ... SELECT`)~~
  — resolved, and the answer is no: confirmed live and via Databricks' own
  error-class docs (`SECRET_FUNCTION_INVALID_LOCATION`). Also confirmed
  that extracting a `secret()`-derived value via `.collect()` doesn't work
  around it — Databricks redacts the SQL command's output first. The
  simplified `hsm_encrypt`/`hsm_decrypt` wrappers only work for ad-hoc
  `SELECT`s; any persisted write must use `_detail` + `dbutils.secrets.get()`
  in the driver instead. See Correction #3 above and `DEPLOYMENT.md`.
- RSA-OAEP unwrap's exact FIPS scope (§8) — small stakes, still worth a direct
  answer before build rather than assuming.
- ~~Where does the compiled wheel get distributed~~ — resolved: a Unity
  Catalog volume path, declared directly in `CREATE FUNCTION`'s `ENVIRONMENT`
  clause (§11) — no per-cluster library attachment needed.
- ~~Whether `dbutils` is available unqualified inside a Unity Catalog Python
  function body~~ — resolved, and the answer is no: confirmed directly
  against Databricks' own docs/KB (and hit live, exactly as predicted, on a
  real deployment attempt) that `dbutils` only works in the calling
  notebook/job's own driver context, never inside a UDF or Unity Catalog
  Python Function body. `hsm_encrypt_detail`/`hsm_decrypt_detail` take a
  caller-supplied `credentials_json` argument instead, built in SQL via the
  native `secret(scope, key)` expression — no `dbutils`/notebook/Python
  needed to call them. See §11 and `DEPLOYMENT.md`.
- ~~Whether a `LANGUAGE SQL` function's body can safely call another
  user-defined SQL function (as a `hsm_credentials()` helper called from
  inside `hsm_encrypt`/`hsm_decrypt`)~~ — resolved, and the answer is no:
  confirmed live that this reliably corrupts Unity Catalog's dependency
  tracking for that specific edge, raising `UC_INVALID_DEPENDENCIES.SQL_UDF`
  on every subsequent call, not fixable by restarting compute or by
  dropping and recreating the whole chain. The same function calling a
  `LANGUAGE PYTHON` function (`hsm_decrypt` → `hsm_decrypt_detail`) is
  reliable — isolated by direct experiment, not assumed. Fixed by inlining
  the credentials expression directly into `hsm_encrypt`/`hsm_decrypt`
  rather than calling a shared SQL helper from their body, at the cost of
  losing a single source of truth for that expression (now duplicated in
  three places). Because `LANGUAGE SQL` functions still default to definer
  rights regardless, the secret-isolation goal (caller needs only
  `EXECUTE`, never `READ SECRET`) is unaffected by this fix. See
  `DEPLOYMENT.md`.
- ~~Auth mode: `SELF_SIGNED_JWT` vs mTLS for the Databricks-side credential~~
  — `SELF_SIGNED_JWT` is implemented (`auth.py`'s `SelfSignedJwtTokenProvider`,
  a Python port of the JVM reference implementation) and verified end-to-end
  against a real `hsm-core-service` instance (`tests/test_live_interop.py`).
  `STATIC` remains the default for simplicity; `mTLS` and `AZURE_AD` are not
  implemented — still depend on the target workspace's network setup /
  identity provider, out of scope until a concrete need arises.
- Whether a Python UDTF (table-valued, batched) is worth building alongside
  the scalar UDF from the start, or only if per-row call overhead proves to
  matter in practice.
