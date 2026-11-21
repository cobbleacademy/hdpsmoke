# De-identification: CSV Export

**Status: phases 1, 2 and 3 built.** Phase 1: the library (`hsm-crypto-client`, package `com.hsm.client.deid`). Phase 2: the `DEID` job (`hsm-bulk-client`, package `com.hsm.client.deidjob`) with JDBC and CSV sources. Phase 3: the JSON source.

Read a database table, CSV file or JSON file holding gift-card reward data.
De-identify it according to a **profile**, and write **one CSV file** plus a
small manifest. The file is then sent to the vendor by the existing transfer
process (MFT/SFTP). This job doesn't deliver files.

> **Not legal advice.** The privacy officer decides, for each vendor, whether
> the file is a *minimum-necessary* disclosure under a BAA or *Safe Harbor*
> de-identified data, and signs off every profile before it runs on
> production data.

## 1. Summary

| | |
|---|---|
| **One run** | One profile → one CSV file + `<file>.manifest.json` |
| **Sources** | Database table or query (JDBC), CSV file, JSON file (JSON Lines or a JSON array) |
| **Output** | CSV (UTF-8, RFC 4180), written to local disk, ADLS or Blob through the existing `hsm-file-store` adapters |
| **Methods** | `drop` (default for anything unlisted), `mask`, `generalize`, `tokenize`, `surrogate`, plus `passthrough` |
| **Vendors** | One profile per vendor. A vendor-specific token key goes in the profile, so tokens differ between vendors. |
| **Where it runs** | `hsm-bulk-client`, new job type `DEID` (`JOB_TYPE=DEID`), as a Kubernetes Job (`helm/hsm-bulk-client-job`) |
| **Where the logic lives** | `hsm-crypto-client`, package `com.hsm.client.deid`, so the Spark adapter can reuse it later |
| **hsm-core changes** | **None.** Keys are ordinary named DEKs, pinned by `edek_id` (section 7). |
| **Encrypted columns** | A column holding HSM ciphertext is marked `decrypt: true`: decrypted in memory, then the method applies (section 6.3) |
| **Surrogate table** | Needed **only when a profile uses `surrogate`.** It lives in the data owner's database (section 8). |
| **Not in scope** | Delivering files to vendors; PGP; a vendor registry; files coming back from vendors; Parquet; unstructured files (section 15) |

## 2. Principles

1. **hsm-core never sees the data.** The job runs in the data owner's
   namespace and calls hsm-core only for keys (`BULK_OPERATIONS.md`).
2. **Deny by default.** A field the profile doesn't list is dropped. A column
   added to the source later can't leak.
3. **Fail before writing.** The profile, keys, source columns and (when
   needed) the surrogate table are checked before the first row is read.
   Nothing is written if any check fails.
4. **Never send why a card was earned.** The program, activity or reason
   behind a reward is health information. Leave it out, or map it to a
   neutral code in the source query.
5. **No values in logs.** Logs, errors and the manifest carry field names,
   counts and row numbers, never values, tokens or surrogates.

## 3. HIPAA basis

Each profile declares one basis. The validator enforces the rules for it
(section 6.2).

| `hipaa-basis` | Typical vendor | Identifiers in the file |
|---|---|---|
| `minimum-necessary` | Fulfillment: sends the cards | Only what delivery needs: name, address or email. Member ID usually as a **token**. A BAA is required. |
| `safe-harbor` | Reporting, reconciliation | The 18 Safe Harbor identifier types removed or reduced. A member ID join key must be a **surrogate**. |
| `expert-determination` | Reporting under an expert's approval | As the expert's report allows; the report reference goes in the profile |

**Why Safe Harbor needs `surrogate` rather than `tokenize`:**

- HIPAA §164.514(c) allows a re-identification code only if it is **not
  derived from information about the individual**.
- An HMAC token is computed from the member ID.
- A surrogate is random.

## 4. Methods

Null in gives null out (except `drop`). The examples all start from this
record:

```
member_id=M1234567  name=Jane Doe  email=jane.doe@mail.com  zip=60614
dob=1958-03-12  card_number=6035712345678901  card_pin=4821  amount=50.00
issued=2026-10-14  program=Diabetes Screening
```

### 4.1 `drop`: remove the field

```
name=Jane Doe   →   (no name column in the output)
```

The default for every field the profile doesn't list.

### 4.2 `mask`: hide part of the value

```
card_number=6035712345678901   →   ************8901
email=jane.doe@mail.com        →   j*******@mail.com
```

| Option | Default | Meaning |
|---|---|---|
| `keep-last` | 0 | Keep the last N characters |
| `keep-first` | 0 | Keep the first N characters |
| `keep` | — | `domain`: email keeps `@domain`. The local part is masked apart from the first `keep-first` characters. |
| `char` | `*` | Mask character |
| `preserve` | `[]` | Characters left as they are, e.g. `["-"]` → `****-****-****-8901` |
| `fixed-length` | — | Output exactly this many mask characters, hiding the real length |

- With no options, the whole value is masked, which also covers "redact".
- A value shorter than `keep-first + keep-last` is masked in full.
- **mask is not de-identification.** The visible part can still identify
  someone, so `safe-harbor` profiles may use it only on fields that aren't
  identifiers.

### 4.3 `generalize`: make the value less precise

```
dob=1958-03-12      →   1958          (to: birth-year)
dob=1933-07-30      →   <=1936        (to: birth-year, run in 2026: the member is 90 or over)
issued=2026-10-14   →   2026          (to: year)
zip=60614           →   606
age=92              →   90+
```

| `to` | Output | Notes |
|---|---|---|
| `year` | `yyyy` | Input: ISO date or date-time, or a configured `input-format`. For dates that aren't birth dates (issue, redemption, service). |
| `birth-year` | `yyyy`, or **`<=yyyy`** when the year shows an age of 90 or over | For birth dates. See "Ages over 89" below. |
| `zip3` | First 3 digits; **`000`** for restricted ZIP3s | The restricted list ships as a reviewed resource. It is the HHS-published list of ZIP3s with ≤ 20,000 people: `036 059 063 102 203 556 692 790 821 823 830 831 878 879 884 890 893`. It can be overridden by config when HHS updates it. |
| `age-90` | The age itself, or **`90+`** | Safe Harbor rule for ages over 89 |
| `age-band` | e.g. `40-49` with `width: 10`, capped at `90+` | |

A value that can't be parsed fails the run. The error gives the field and row
number, never the value.

**Ages over 89 (`birth-year`).** Safe Harbor removes ages over 89 *and every
date element that shows such an age, the year included*
(§164.514(b)(2)(i)(C)). A birth year of 1933 in a 2026 file tells the vendor
the member is 93, so `year` is not enough for a birth date. `birth-year`
works like this:

- **Cut-off year** = the run's year (UTC) − 90. In 2026 that is **1936**.
- A birth year **after** the cut-off is written as is: `1958`.
- A birth year **at or before** the cut-off is written as `<=1936`. Everyone
  aged 90 or over falls in one bucket, so the file shows only "90 or older".
- **It compares years only,** without month and day. Someone born in late
  1936 who is still 89 is also written as `<=1936`. That errs on the side of
  hiding.
- **Other options:**
  - `over-89-as: "90+"` writes `90+` instead of `<=1936`;
  - `over-89-as: ""` writes an empty value.
- **`as-of: <field>`** measures age at another date in the same record (say
  `issued`) instead of the run date. The run date is the default because it
  hides the most: ages only go up. **`safe-harbor` profiles reject `as-of`**,
  because someone 85 at issue in 2020 is 91 today.
- **Pair with `age`.** If the profile also exports an `age` field, it must
  use `age-90` or `age-band`, which cap at `90+` the same way.

### 4.4 `tokenize`: a consistent code *computed* from the value

```
member_id=M1234567   →   tk_Q2x9fP0aLmV3kR7tYw1bZg
```

```
token = "tk_" + base64url_nopad( HMAC-SHA256( K_tok, domain ‖ 0x00 ‖ normalize(value) )[0..16) )
```

- **Consistency:** the same value, key and domain always give the same
  token, across runs.
- **Not reversible.** The key comes from the profile's `token-key-edek-id`
  (section 7). Give each vendor its own key so tokens differ between vendors.
- **`domain`:** set per field, or the profile's `token-domain`. Fields share
  tokens only within one domain.
- **`normalize`:** `none`, `trim` (default), `lower`, or `digits` (keep only
  the digits).
- **Format:** `tk_` + 22 characters. The `prefix` can be changed.
- **Rejected on identifier fields in `safe-harbor` profiles** (section 3).

### 4.5 `surrogate`: a *random* ID, kept in a table

```
member_id=M1234567   →   S-8F2KQ-91QDM
deid_surrogate (stays with us): (scope=zen-reporting, domain=member, M1234567) ↔ S-8F2KQ-91QDM
```

- The first time a value is seen in a **scope**, a random surrogate is
  created and stored. Later runs reuse it.
- The scope is the profile's `surrogate-scope`, normally one per vendor.
- **The vendor can't reverse it.** We can map it back through the table
  (section 8).
- **Format:** `S-` + 10 Crockford base32 characters from `SecureRandom`
  (50 bits), grouped 5-5. The prefix and length can be configured.
- **Allowed for every basis.** It is the method to use for identifier fields
  in `safe-harbor` profiles that need a join key.

### 4.6 `passthrough`: copy unchanged

How a field is explicitly allowed through.

### 4.7 Never-export fields

Card PINs, CVVs and activation or security codes are **never** exported:

- A source column or JSON path matching `forbidden-fields` must be `drop`,
  or not listed at all. Any other method fails validation.
- The default pattern is
  `(?i)(^|[_.\-\]])(pin|pin_?code|cvv2?|cvc2?|activation(_?code)?|security_?code|passcode)($|[_.\-\[])`.
  It matches a whole part of the name, so `card_pin` and `pin` match but
  `shipping_zip` doesn't.
- The output header is checked again before writing.

## 5. How it runs

```
 data owner's namespace — hsm-bulk-client (K8s Job, JOB_TYPE=DEID)

 source ──► DeidPipeline (profile: field → method) ──► CSV file + manifest ──► hand-off to MFT/SFTP
 · JDBC              │
 · CSV file          ├── TokenKeyProvider ── /dek/unwrap ──► hsm-core-service   (if tokenize / surrogate)
 · JSON file         ├── FieldDecryptor ──── /dek/unwrap ──► hsm-core-service   (only if decrypt: true)
                     └── SurrogateStore ──── deid_surrogate (owner's DB)          (only if surrogate)
```

Steps:

1. **Validate:** the profile, method options, never-export fields, the
   basis rules, and the source columns or JSON paths (from the header,
   table metadata or a first-record sample).
2. **Load keys** for the profile's methods:
   - `tokenize` → the token key;
   - `surrogate` → the surrogate-table key, and a check that the table
     exists;
   - `decrypt: true` → decrypt the first non-null value of each such field
     from a sample, so a missing grant fails here, before anything is
     written.
3. **Stream records** in pages: decrypt the `decrypt: true` fields, transform
   them, resolve surrogates for the whole page at once, and write CSV rows.
4. **Write `<file>.manifest.json`** and the audit event.

The CSV and manifest are written as `<name>.part` and renamed when complete,
so a transfer process watching the folder never picks up a half-written file.

## 6. Profile

### 6.1 Format

```yaml
# deid-profiles/acme-fulfillment.yml
id: acme-fulfillment
version: 1
vendor: acme-rewards                   # label for the manifest and audit only
hipaa-basis: minimum-necessary
token-key-edek-id: 6f1c…               # Acme's token key (section 7); needed only if tokenize is used
token-domain: member
fields:                                 # anything not listed is dropped; output columns follow this order
  - { name: member_id, phi: member-id, method: tokenize }
  - { name: name,      phi: name,      method: passthrough }
  - { name: address,   phi: address,   method: passthrough }
  - { name: zip,       phi: zip,       method: passthrough }
  - { name: email,     phi: email,     method: passthrough }
  - { name: amount,                    method: passthrough }
  - { name: card_type,                 method: passthrough }
output:
  file: "giftcards_acme_{yyyyMMdd}.csv"
```

```yaml
# deid-profiles/zen-reporting.yml
id: zen-reporting
version: 1
vendor: zen-analytics
hipaa-basis: safe-harbor
surrogate-scope: zen-analytics          # needed only if surrogate is used
fields:
  - { name: member_id,   phi: member-id,   method: surrogate, domain: member }
  - { name: card_number, phi: card-number, method: surrogate, domain: card }
  - { name: dob,         phi: birth-date,  method: generalize, to: birth-year }
  - { name: issued,      phi: date,        method: generalize, to: year }
  - { name: zip,         phi: zip,         method: generalize, to: zip3 }
  - { name: amount,                        method: passthrough }
  - { name: status,                        method: passthrough }
output:
  file: "giftcards_zen_{yyyyMMdd}.csv"
  rename: { member_id: member_ref }     # optional output column names
```

- A JSON source uses paths in `name`: `customer.ssn`, `orders[].card_number`
  (section 9.1). Output columns drop the `[]`: `orders.card_number`, unless
  renamed. A path that leads to an object or array, rather than a single
  value, is a bad record. In YAML, quote a path with `[]`
  (`name: "orders[].card_number"`): unquoted, `[` starts a list.
- Profiles live in version control. Changes go through pull requests
  reviewed by the data owner and the privacy officer.
- The manifest records each profile's SHA-256.

### 6.2 Validator rules

Fields holding Safe Harbor identifiers carry a `phi:` category. Under
`safe-harbor`:

| `phi` category | Allowed methods |
|---|---|
| `name`, `address`, `email`, `phone`, `fax`, `ssn`, `url`, `ip`, `device-id`, `license`, `vehicle-id`, `biometric`, `photo`, `free-text` | `drop` only |
| `member-id`, `mrn`, `account`, `card-number`, `plan-id`, other unique IDs | `drop`, `surrogate` |
| `birth-date` | `drop`, `generalize: birth-year` (`year` is **rejected**: it would show ages over 89) |
| `date` (service, issue, redemption) | `drop`, `generalize: year` |
| `zip` | `drop`, `generalize: zip3` |
| `age` | `drop`, `generalize: age-90` / `age-band` |
| *(none)* | any |

Further rules:

- **`minimum-necessary`** allows any method on any field. Its review happens
  at profile approval, not in code.
- **`expert-determination`** must carry `expert-report: <ref>`.
- **Never-export fields** (section 4.7) apply to every basis.
- **A `tokenize` field** requires `token-key-edek-id`. **A `surrogate`
  field** requires `surrogate-scope` and the surrogate table.
- **`decrypt: true`** is rejected with `drop` (there's nothing to decrypt
  for) and on never-export fields. With `passthrough` it sends the
  plaintext, so it's allowed only where `passthrough` itself is allowed.
- **A column that looks encrypted** (its sampled values are `v1.` tokens)
  but isn't marked `decrypt: true` is flagged by the dry run. Sending
  ciphertext is useless to the vendor and reveals our `edek_id`s.
- **The validator can't detect an identifier that isn't tagged with `phi`.**
  To help the reviewer:
  - the dry run lists every passthrough field;
  - the manifest lists every source column dropped by default.

### 6.3 Encrypted columns: `decrypt: true`

Some source columns hold **HSM ciphertext**, not the value itself: the
packed `v1.…` token returned by hsm-core's `/encrypt` or by
`HsmCryptoClient.encrypt`. Masking or tokenizing that token would only
transform the ciphertext. Mark such a field `decrypt: true`:

```yaml
# a minimum-necessary profile: mask on email isn't allowed under safe-harbor
fields:
  - { name: card_number, phi: card-number, decrypt: true, method: mask, keep-last: 4 }
  - { name: email,       phi: email,       decrypt: true, method: mask, keep: domain, keep-first: 1 }
  - { name: member_id,   phi: member-id,   decrypt: true, method: surrogate, domain: member }
  - { name: amount,                                        method: passthrough }
```

```
card_number = v1.Zm9v…(ciphertext)   →  decrypt → 6035712345678901  →  mask → ************8901
```

How it works:

- **Decrypt, then transform.** The job calls `HsmCryptoClient.decryptToString`
  on the value, then applies the field's method to the plaintext. The
  method sees exactly what it would see for an unencrypted column.
- **Keys come from the value.** Each token carries its own `edek_id`. The
  client's decrypt cache makes one `/dek/unwrap` per `edek_id` per TTL,
  not one per row. No key setting goes in the profile.
- **hsm-core still never sees the data.** Decryption runs in the job, in
  the data owner's namespace. Only the wrapped DEK crosses to hsm-core
  (principle 1).
- **Plaintext stays in memory,** one page at a time. It is never written
  to disk, the manifest or logs. Byte arrays are zeroed after use. Java
  `String`s can't be zeroed, so plaintext strings live until garbage
  collection, as in the existing DB bulk job (`DbBulkJob`).
- **Null or empty** stays null or empty and is not decrypted.
- **A value that isn't a token, or fails authentication** (tampered,
  wrong key, KEK version disabled), fails the run. The error names the
  field and row number, never the value. Nothing is left behind (no
  `.csv`).
- **Rotated rows still decrypt.** `/dek/unwrap` resolves by `edek_id`
  whatever the rotation status. Only a disabled or deleted KEK version
  breaks it (`KEK_LIFECYCLE.md`).

**Access:** the job's `app_id` needs the `dek_unwrap` scope, and must own the
DEKs that encrypted the column or hold a **decrypt grant** on them
(section 11.1). The data is usually encrypted by the owning application,
so the owner grants decrypt, one `app_dek_grants` row per column, to the
export's own `app_id`.

## 7. Keys

Keys are **named DEKs issued by hsm-core**, pinned by `edek_id`. At startup
the job:

1. unwraps each needed key once with `/dek/unwrap`;
2. derives a purpose key with `HKDF-SHA256(DEK, salt = none, info = <label>, L = 32)`
   (RFC 5869; no salt means 32 zero bytes);
3. zeroes the DEK.

HMAC, HKDF and AES-GCM all run in BC-FIPS.

| Key | Named DEK | Where it's set | HKDF label | Needed when |
|---|---|---|---|---|
| Token key | `deid-<app>-<vendor>-tok` (one per vendor) | Profile: `token-key-edek-id` | `hsm-deid-tokenize-v1` | The profile uses `tokenize` |
| Surrogate-table key | `deid-<app>-surrogate` (one per data owner) | Job config: `client.deid.surrogate.key-edek-id` | `hsm-deid-surrogate-lookup-v1`, `hsm-deid-surrogate-enc-v1` | The profile uses `surrogate` |

**Creating a key.** `hsm-bulk-client --deid.init-key --name <dek-name>` calls
`/dek/issue` and prints the `edek_id`.

**Why pin by `edek_id`:**

- Named-DEK rotation (720 hours by default) eventually marks the pinned row
  `ROTATED` and creates a new DEK under the same name.
- `/dek/unwrap` resolves by `edek_id` whatever the rotation status
  (`DekUnwrapService`). The pinned key keeps working and tokens never change
  by accident.
- **This is why hsm-core needs no change.**

**Changing a vendor's token key** is a deliberate step: create a new key,
update the profile, raise `token-epoch`, tell the vendor.

**Access:** the job's `app_id` must own the DEK or hold a decrypt grant on it,
with the `dek_unwrap` scope (and `dek_issue` to create keys); see
section 11.1.

**Not in this table:** the DEKs behind `decrypt: true` fields. Those are the
source data's own DEKs, found from each value's `edek_id` (section 6.3).

**KEK lifecycle:** a pinned row that becomes `ROTATED` stays on its original
KEK version. **Never disable that version** (`KEK_LIFECYCLE.md` §4.3 counts
`ROTATED` rows).

## 8. Surrogate table (only when `surrogate` is used)

A profile without `surrogate` never touches this table. The job checks that
the table exists only when a profile needs it.

### 8.1 Where it lives

In the **data owner's database**, reached through the job's JDBC connection.
Not in hsm-core. Back it up with the owner's other Tier 0 data: losing it
means surrogates can no longer be mapped back.

### 8.2 Table

```sql
CREATE TABLE deid_surrogate (
    scope          varchar(64)  NOT NULL,   -- profile's surrogate-scope, normally the vendor
    domain         varchar(64)  NOT NULL,   -- e.g. member, card
    lookup_hash    bytea        NOT NULL,   -- HMAC(K_lookup, domain ‖ 0x00 ‖ normalize(value)), 32 bytes
    value_enc      bytea        NOT NULL,   -- IV ‖ AES-GCM(K_enc, normalize(value)) ‖ tag, AAD binds scope and domain
    key_edek_id    uuid         NOT NULL,   -- which surrogate-table key encrypted value_enc
    surrogate      varchar(64)  NOT NULL,
    created_at     timestamptz  NOT NULL DEFAULT now(),
    created_run_id varchar(64)  NOT NULL,
    PRIMARY KEY (scope, domain, lookup_hash),
    UNIQUE (scope, domain, surrogate)
);
```

- **Lookups without storing values in clear:** `lookup_hash` lets rows be
  found by equality, and the value itself is never stored unencrypted.
- **Mapping back:** `value_enc` holds the original value, encrypted, so a
  surrogate can be mapped back on request.
- **Provisioning:** the DDL ships in the jar as `deid/deid-surrogate-postgres.sql`,
  `deid-surrogate-sqlserver.sql` and `deid-surrogate-oracle.sql`. The owner's DBA or
  Flyway applies it; the job never creates tables. Postgres, SQL Server and
  Oracle variants are provided, matching the bulk-client's existing database
  dialects.

### 8.3 Behaviour

For each page of records, the job:

1. computes the lookup hashes;
2. fetches the existing rows for those hashes;
3. inserts the missing ones with new random surrogates. The insert is a plain
   `INSERT`: when it breaks the primary key (another run inserted the row) or
   the unique surrogate constraint (a collision), the database refuses it and
   a read says which. This works the same on all three databases;
4. re-reads the rows whose insert lost a race.

Two runs for the same scope therefore always agree on one surrogate.

- **Collisions:** a random value that collides with an existing surrogate
  (unique constraint) is retried with a new value, up to 5 times. After that
  the run fails.
- **Retention:** keep a member's rows for as long as the member is needed
  (section 16). Deleting rows means the files already sent can never be
  mapped back for those members.

## 9. Sources

| Source | Settings | Notes |
|---|---|---|
| **JDBC** | The existing `client.db.source` (`table` or `query`, `key-column`, `row-batch-size`) | Pages by key. A `query` can also map the program or reason to a neutral code. |
| **CSV** | `header: true` (required), `delimiter`, `quote`, `encoding: UTF-8` (BOM accepted) | Values are read as strings. A wrong column count fails by default; `on-bad-row: skip` counts it instead. |
| **JSON** | `layout: lines \| array`, streamed | Field paths as in section 6.1 |

- **Files:** read from local disk, ADLS or Blob through `hsm-file-store`, as
  the existing FILE job does.
- **HSM-encrypted input files** (encrypted file format v1 or v2) are
  decrypted in memory first. That needs a decrypt grant.
- **HSM-encrypted columns or fields** inside any source (a table column, a
  CSV column, a JSON field) are marked `decrypt: true` in the profile
  (section 6.3). A whole encrypted file and encrypted fields inside it can
  be combined.
- **Page size:** `client.db.row-batch-size`, for both sources.
- **Errors** name the row or record number. A `key-field` may replace it,
  but only a non-sensitive column, because it is printed in logs.

### 9.1 JSON source

```yaml
client.deid.source:
  type: JSON
  json:
    store: { type: LOCAL, root: /data/in }   # or ADLS / AZURE_BLOB
    path: members.jsonl
    layout: LINES        # LINES: one object per line (JSON Lines) | ARRAY: one top-level [ ... ]
    encoding: UTF-8      # a BOM is accepted
    on-bad-row: FAIL     # or SKIP: counted in the manifest
    encrypted: false     # true: the whole file is HSM-encrypted (v1/v2)
```

- **Paths.** `a.b` reads a nested key. A missing key or a JSON `null` is
  null. Strings are read as they are, numbers keep their source text
  (`25.00` stays `25.00`), booleans become `true` / `false`.
- **One array can be expanded.** `cards[].card_number` reads every element
  of `cards`; each element becomes one CSV row that repeats the record's
  other fields. Every `[]` in a profile must be the same array, once per
  path (`a[].b[].c` and two different arrays are rejected: they would
  multiply rows). `tags[]` reads an array of plain values. A missing, null
  or empty array gives **one** row with the array fields empty, so no
  member drops out of the file.
- **Row numbers** in a transform error count CSV rows (after expanding), not
  JSON records.
- **Bad records** (not an object, invalid JSON, a repeated key, a path that
  reaches an object or array or passes through a non-object) fail the run,
  naming the record number and, for `LINES`, the line; or are counted and
  skipped with `on-bad-row: SKIP`. Invalid JSON in an `ARRAY` file always
  fails, because the parser can't find the next record. Messages never quote
  the source text.
- **Columns.** There's no header, so the validator checks the profile's paths
  against themselves (a typo can't be caught up front). Instead, a path no
  record has is listed as `paths_not_seen` in the dry-run review and logged
  as `deid_json_paths_not_seen` after a real run. The manifest's
  `dropped_by_default` lists every value path seen in the file
  (`name.last`, `cards[].pin`), up to 2,000 paths.

## 10. Output

### 10.1 CSV file

- UTF-8 with no BOM, RFC 4180 quoting, a header row, `\n` line endings.
  Override per profile with `output.delimiter` / `output.line-ending: crlf`.
- Columns follow the profile's field order, with optional `rename`.
- Every value is written as a string. Numbers and dates appear exactly as
  produced by the source, or by the method.
- The file name comes from `output.file`; the placeholders are `{yyyyMMdd}`,
  `{run}` and `{profile}`. The location is `client.deid.output.root`, a
  local folder: in Kubernetes, a mounted volume the client's MFT picks up
  from. Writing straight to ADLS or Blob isn't built: the `.part` rename
  needs a local file system (a CSI-mounted container works).
- An existing file of the same name is never overwritten; the run fails.
  Use `{run}` in the name for more than one run a day.
- **Excel-safe text:** a value beginning with `=`, `+`, `-`, `@`, tab or
  carriage return gets a leading `'`, so spreadsheets don't run it as a
  formula. The setting is `output.formula-guard: true`, on by default.

### 10.2 Manifest

`<file>.manifest.json` (e.g. `giftcards_zen_20261120.manifest.json`), written
beside the CSV after the CSV is complete. It contains no values, tokens or
surrogates.

```json
{
  "run_id": "deid-20261120-1530-7f3a",
  "profile": { "id": "zen-reporting", "version": 1, "sha256": "…", "vendor": "zen-analytics",
               "hipaa_basis": "safe-harbor" },
  "triggered_by": "<sub / pipeline id>",
  "started_at": "…", "completed_at": "…",
  "keys": { "token": null, "surrogate": { "edek_id": "…" } },
  "fields": [
    { "source": "member_id", "output": "member_ref", "phi": "member-id", "method": "surrogate@v1", "domain": "member",
      "decrypted": true },
    { "source": "dob",       "output": "dob",        "phi": "birth-date", "method": "generalize@v1", "to": "birth-year",
      "cutoff_year": 1936, "capped": 412 },
    { "source": "amount",    "output": "amount",     "method": "passthrough" }
  ],
  "dropped_by_default": ["name", "email", "phone", "program", "card_pin"],
  "surrogates": { "new": 1840, "existing": 118591 },
  "decrypt": { "fields": ["member_id"], "distinct_edek_ids": 3 },
  "output": { "file": "giftcards_zen_20261120.csv", "rows_in": 120431, "rows_out": 120431,
              "rows_skipped": 0, "sha256": "…" }
}
```

The transfer process sends only the CSV. The manifest stays with us unless a
vendor asks for it.

- Each field also carries `values` (records read) and `empty` (null or
  empty), and the method's counters.
- `surrogates.new` / `.existing` count distinct values per page, so a value
  that appears on two pages counts twice. They show the scale of new
  members, not an exact count.

### 10.3 Dry run

`--dry-run --sample 100` (or `DEID_DRY_RUN=true`, `DEID_SAMPLE`) validates,
reads a sample and transforms it, but writes nothing: no CSV, no manifest,
and no surrogate rows (new surrogates stay in memory for the sample). It
prints one JSON document to standard output:

- the manifest;
- per field: method, `phi` category, null rate, and the distinct count in
  the sample;
- the passthrough fields and the columns dropped by default;
- `ciphertext_not_marked_decrypt`: listed fields whose sample values are
  HSM ciphertext but that aren't `decrypt: true`. A real run fails on these.

It never prints values. Use it when reviewing a new or changed profile.

### 10.4 Audit

One log event per run on logger `com.hsm.client.deidjob.audit`:
`deid_export_completed` (or `deid_export_failed`, with the reason), with:

- the profile id, version and hash;
- the vendor label and basis;
- the row count, the file name and its SHA-256.

This is the record of what was produced for whom. The transfer system
records the delivery itself.

## 11. Running it

Each profile is a Kubernetes Job (or CronJob) from the existing
`helm/hsm-bulk-client-job` chart. The values set:

- `JOB_TYPE=DEID`;
- `DEID_PROFILE=/deid-profiles/<profile>.yml`, mounted from a ConfigMap;
- `DEID_OUTPUT_ROOT`, the mounted MFT pick-up volume.

The chart sets these from `deid.enabled`, `deid.profilesConfigMap`,
`deid.profile` and `deid.output.*`. The rest (svc, `client.db.source`,
`client.deid.source`, `client.deid.surrogate.key-edek-id`) goes in the job's
`job.yml` Secret, as for the other jobs; start from
`config-examples/deid-csv-export-example.yml`.

Two vendors means two Jobs with different `DEID_PROFILE` values. Exit code:

- `0` on success;
- non-zero on any validation or processing failure, and no `.csv` is left
  behind.

### 11.1 Registration in hsm-core

The job calls hsm-core as an `app_id`, like any other app, so that
`app_id` must be onboarded first (`APP_ONBOARDING.md`). **Exception:** a
profile that uses only `drop`, `mask`, `generalize` and `passthrough` on
plain (not encrypted) columns never calls hsm-core and needs no
registration.

**Recommended: a separate `app_id` for the export** (e.g.
`giftcard-deid-export`), not the owning application's own `app_id`:

- the audit trail shows exports apart from the application's normal
  traffic;
- the onboarding team can revoke or deactivate the export
  (`/admin/apps/status`) without touching the application.

**What to register:**

| What | Where | How | Needed when |
|---|---|---|---|
| The `app_id` row, `active = TRUE` | `app_registrations` | A Flyway migration (`APP_ONBOARDING.md` Step 1); never a hand-run `INSERT` | Always (if hsm-core is called) |
| `allowed_scopes` | `app_registrations` | Same migration; see the table below | Always |
| Encryption public key (`public_key_pem`) | `app_registrations` | `POST /admin/apps/keys` by an operator holding `provision_app_keys`. The job keeps the private half (`svc.private-key-pem`). | Always: `/dek/issue` and `/dek/unwrap` return the DEK wrapped under this key, and refuse an app without one |
| Authentication | Entra ID, or `app_registrations` | `AZURE_AD`: an Entra app registration whose token carries the `app_id`. `SELF_SIGNED_JWT`: signing public key via `POST /admin/apps/keys`. `MTLS`: certificate fingerprint via `POST /admin/apps/mtls-cert`. | Always |
| Decrypt grant on the owner's DEKs | `app_dek_grants` | Same or a later migration | `decrypt: true` on data another app encrypted |

**Endpoint scopes (`allowed_scopes`).** Each endpoint requires one scope
(`hsm.security.access-rules`); a missing scope gets 403.

| Profile uses | Calls | `allowed_scopes` |
|---|---|---|
| `tokenize`, `surrogate`, and/or `decrypt: true` | `/dek/unwrap` | `dek_unwrap` |
| Creating its own keys (`--deid.init-key`) | `/dek/issue` | add `dek_issue`; it can be removed once the keys exist |
| Only `drop` / `mask` / `generalize` / `passthrough` on plain columns | none | no registration |

The job never needs `encrypt` or `decrypt` (those are the per-value
`/encrypt` and `/decrypt` endpoints) or any admin scope.

**Endpoint scope is not grant scope:**

- `allowed_scopes` decides **which endpoints** the app may call.
- The `scope` column of `app_grants` / `app_dek_grants` (`'encrypt'` or
  `'decrypt'`) decides **whose DEKs** it may use. `/dek/unwrap` checks a
  `'decrypt'` grant for every DEK the app doesn't own.
- The job's own keys (token key, surrogate-table key) are owned by the
  job's `app_id`, so they need no grant. Only `decrypt: true` on another
  app's data does.
- Use **one fine-grained `app_dek_grants` row per encrypted column**. A
  coarse `app_grants` row would let the export decrypt every DEK the owner
  has.

**Example migration:**

```sql
-- V{n}__add_app_giftcard_deid_export.sql
INSERT INTO ${access_schema}.app_registrations (app_id, allowed_scopes, description, active, created_at, updated_at)
VALUES ('giftcard-deid-export', 'dek_issue,dek_unwrap',
        'DEID CSV export to vendors - owner: <team / contact>', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Only for decrypt: true columns encrypted by another app: one row per column (dek_name).
INSERT INTO ${access_schema}.app_dek_grants (grantee_app_id, owner_app_id, dek_name, scope, created_at)
VALUES ('giftcard-deid-export', 'giftcard-svc', 'giftcards.card_number', 'decrypt', CURRENT_TIMESTAMP);
INSERT INTO ${access_schema}.app_dek_grants (grantee_app_id, owner_app_id, dek_name, scope, created_at)
VALUES ('giftcard-deid-export', 'giftcard-svc', 'giftcards.email', 'decrypt', CURRENT_TIMESTAMP);
```

**When creating keys with `--deid.init-key`:**

- **DEK names are global.** `deid-<app>-<vendor>-tok` must not already
  belong to another app.
- **Name reservation** (`DEK_NAME_RESERVATION_ENFORCE`, off by default)
  rejects a first mint only if *another* app has reserved that exact name
  in `kek_registry`. An app minting its own new name is never blocked.
- **Classification governance** (`CLASSIFICATION_GOVERNANCE_ENFORCE`, off
  by default): if the key is issued with a `data_classification` and
  enforcement is on, the `(app_id, classification)` pair must first be
  approved (`/admin/apps/classifications`, scope
  `manage_classifications`).

**Checklist** for the onboarding team, before the first run:

1. Migration merged and deployed: `app_id`, scopes, grants.
2. Public key provisioned (`POST /admin/apps/keys`); authentication set up.
3. Keys created with `--deid.init-key` if the profile uses `tokenize` or
   `surrogate`; their `edek_id`s go into the profile or job config.
4. A dry run (`--dry-run --sample 100`) passes: it decrypts a sample of each
   `decrypt: true` field, so a missing grant or key fails here.

## 12. Testing

| Test | Covers |
|---|---|
| Test vectors (`deid-test-vectors.json`) | Tokenize, the HKDF labels and lookup hashes for a fixed key and inputs, so the algorithms can't drift |
| `mask` / `generalize` tables | Every option; short values; ZIP+4; restricted ZIP3 → `000`; `90+`; an unparseable date fails |
| Ages over 89 | `birth-year` run in 2026: 1937 → `1937`, 1936 → `<=1936`, 1933 → `<=1936`; `over-89-as`; `as-of` another field (rejected under `safe-harbor`); a fixed clock so the cut-off is tested at a year boundary; `safe-harbor` rejects `phi: birth-date` with `to: year` |
| Deny by default | An unlisted column or JSON path never appears in the CSV |
| Never-export | `card_pin` with any method other than `drop` → validation fails |
| Safe Harbor validator | Each row of section 6.2, e.g. `tokenize` on `member-id` and `mask` on `email` are rejected |
| Per-vendor keys | Same member, two profiles with different token keys → different tokens |
| Surrogate table | Reuse across runs; two concurrent runs agree; a collision is retried; mapping back decrypts the original; a profile without `surrogate` never touches the table (works with no table present) |
| Pinned key after rotation | Mark the token DEK `ROTATED`; tokens don't change |
| `decrypt: true` | Encrypted column → mask/tokenize/surrogate give the same output as the plaintext column; null stays null; a non-token or tampered value fails naming field and row only; no decrypt grant fails at validation, before writing; a `ROTATED` DEK still decrypts; an unmarked `v1.` column is flagged by the dry run |
| CSV output | Quoting, formula guard, `.part` then rename, column order and `rename` |
| No values in logs | Logs captured during a run with known sensitive inputs contain none of them |
| End to end | H2 and Postgres (Testcontainers): table → fulfillment CSV and reporting CSV → manifests |

## 13. Phases

| Phase | Deliverable |
|---|---|
| **1. `hsm-crypto-client`** | The `deid` package: the six methods, `FieldDecryptor` (`decrypt: true`), the `SurrogateStore` interface, `TokenKeyProvider` (HKDF), profile parsing and validation, test vectors |
| **2. `hsm-bulk-client`** | The `DEID` job with JDBC and CSV sources; CSV output; manifest; audit; dry run; `--deid.init-key`; JDBC surrogate store with its DDL; example profiles; Helm values example |
| **3. JSON** | The JSON source (`lines` / `array`, field paths) |

## 14. Files

Planned new (**A**) and changed (**M**) files, with paths relative to the
repository root.

### Phase 1: `java/hsm-crypto-client`

| | Path |
|---|---|
| M | `java/hsm-crypto-client/pom.xml` (`snakeyaml`, test scope only) |
| M | `java/hsm-crypto-client/src/main/java/com/hsm/client/HsmCryptoClient.java` (`unwrapDek`, the source of the tokenize and surrogate keys) |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/ColumnTransform.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/FieldOptions.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/FieldStats.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/DeidException.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/DeidValidationException.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/TransformContext.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/TransformRegistry.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/DeidProfile.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/HipaaBasis.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/PhiCategory.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/ProfileValidator.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/DeidPipeline.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/TokenKeyProvider.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/SurrogateStore.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/FieldDecryptor.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/transform/DropTransform.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/transform/MaskTransform.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/transform/GeneralizeTransform.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/transform/TokenizeTransform.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/transform/SurrogateTransform.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/transform/PassthroughTransform.java` |
| A | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/transform/Normalize.java` |
| A | `java/hsm-crypto-client/src/main/resources/com/hsm/client/deid/restricted-zip3.txt` |
| A | `java/hsm-crypto-client/src/test/java/com/hsm/client/deid/MaskTransformTest.java` |
| A | `java/hsm-crypto-client/src/test/java/com/hsm/client/deid/GeneralizeTransformTest.java` |
| A | `java/hsm-crypto-client/src/test/java/com/hsm/client/deid/TokenizeTransformTest.java` |
| A | `java/hsm-crypto-client/src/test/java/com/hsm/client/deid/SurrogateTransformTest.java` |
| A | `java/hsm-crypto-client/src/test/java/com/hsm/client/deid/ProfileValidatorTest.java` |
| A | `java/hsm-crypto-client/src/test/java/com/hsm/client/deid/DeidPipelineTest.java` |
| A | `java/hsm-crypto-client/src/test/java/com/hsm/client/deid/FieldDecryptorTest.java` |
| A | `java/hsm-crypto-client/src/test/java/com/hsm/client/deid/TokenKeyProviderTest.java` |
| A | `java/hsm-crypto-client/src/test/java/com/hsm/client/deid/DeidTestSupport.java` |
| A | `java/hsm-crypto-client/src/test/resources/deid-test-vectors.json` |

Notes on what phase 1 built:

- **Profiles are read from maps** (`DeidProfile.fromMap`), so the library has
  no YAML dependency. The bulk client reads the YAML file and hashes its
  bytes for the manifest.
- **Test vectors are computed independently** (Python `hmac`/`hashlib`), not
  by the Java code, and the HKDF test also checks the published RFC 5869 A.3
  output.
- **The surrogate algorithm lives in `SurrogateTransform`**: fetch, insert
  the missing rows, re-read lost races, retry collisions. A `SurrogateStore`
  only does plain reads and inserts, so every database behaves the same.

### Phase 2: `java/hsm-bulk-client`

| | Path |
|---|---|
| M | `java/hsm-bulk-client/src/main/java/com/hsm/client/ClientApplication.java` (dispatches `DEID`, `--deid.init-key`, `--dry-run`, `--sample`) |
| M | `java/hsm-bulk-client/src/main/java/com/hsm/client/config/ClientProperties.java` (the `client.deid.*` block) |
| M | `java/hsm-bulk-client/src/main/java/com/hsm/client/db/DbDialect.java` (`resolve` made public) |
| M | `java/hsm-bulk-client/src/main/resources/application.yml` (the `DEID_*` settings) |
| M | `java/hsm-crypto-client/src/main/java/com/hsm/client/HsmCryptoClient.java` (`fromConfig(SvcConfig)`) |
| A | `java/hsm-bulk-client/src/main/java/com/hsm/client/deidjob/DeidJob.java` |
| A | `java/hsm-bulk-client/src/main/java/com/hsm/client/deidjob/DeidInitKeyCommand.java` |
| A | `java/hsm-bulk-client/src/main/java/com/hsm/client/deidjob/DeidManifest.java` |
| A | `java/hsm-bulk-client/src/main/java/com/hsm/client/deidjob/source/RecordSource.java` |
| A | `java/hsm-bulk-client/src/main/java/com/hsm/client/deidjob/source/JdbcRecordSource.java` |
| A | `java/hsm-bulk-client/src/main/java/com/hsm/client/deidjob/source/CsvRecordSource.java` |
| A | `java/hsm-bulk-client/src/main/java/com/hsm/client/deidjob/sink/CsvRecordSink.java` |
| A | `java/hsm-bulk-client/src/main/java/com/hsm/client/deidjob/store/JdbcSurrogateStore.java` |
| A | `java/hsm-bulk-client/src/main/resources/deid/deid-surrogate-postgres.sql` |
| A | `java/hsm-bulk-client/src/main/resources/deid/deid-surrogate-sqlserver.sql` |
| A | `java/hsm-bulk-client/src/main/resources/deid/deid-surrogate-oracle.sql` |
| A | `java/hsm-bulk-client/config-examples/deid-profiles/acme-fulfillment.yml` |
| A | `java/hsm-bulk-client/config-examples/deid-profiles/zen-reporting.yml` |
| A | `java/hsm-bulk-client/config-examples/deid-csv-export-example.yml` |
| A | `java/hsm-bulk-client/src/test/java/com/hsm/client/deidjob/DeidJobTest.java` |
| A | `java/hsm-bulk-client/src/test/java/com/hsm/client/deidjob/DeidJobTestSupport.java` |
| A | `java/hsm-bulk-client/src/test/java/com/hsm/client/deidjob/CsvRecordSourceTest.java` |
| A | `java/hsm-bulk-client/src/test/java/com/hsm/client/deidjob/CsvRecordSinkTest.java` |
| A | `java/hsm-bulk-client/src/test/java/com/hsm/client/deidjob/JdbcSurrogateStoreTest.java` |
| A | `java/hsm-bulk-client/src/test/java/com/hsm/client/deidjob/DeidContainerTest.java` |
| M | `helm/hsm-bulk-client-job/values.yaml` (the `deid` block) |
| M | `helm/hsm-bulk-client-job/templates/job.yaml` (DEID env, profile ConfigMap and output volume) |
| M | `helm/hsm-bulk-client-job/templates/NOTES.txt` |
| M | `java/docs/RUNBOOK.md` (a "De-identification export (DEID job)" section) |

Notes on phase 2 as built:

- **No new dependency.** The CSV reader and writer are small RFC 4180
  classes in the module, not `jackson-dataformat-csv`. The profile YAML is
  read with SnakeYAML, which Spring Boot already brings.
- **`DeidContainerTest`** covers the shipped DDL and `JdbcSurrogateStore` on
  Postgres, SQL Server and Oracle, and the whole job on Postgres. Like
  `CheckpointStoreTest` it's skipped without Docker. The other tests run on
  H2 with the Postgres DDL.
- **Unmarked ciphertext fails a real run** (not only flagged by the dry
  run): exporting ciphertext is useless to the vendor and shows our
  `edek_id`s.
- **`--deid.init-key`** only accepts names of the form `deid-<…>` (lower
  case, digits, hyphens), so a key can't take an application DEK's name.

### Phase 3: JSON

| | Path |
|---|---|
| A | `java/hsm-bulk-client/src/main/java/com/hsm/client/deidjob/source/JsonRecordSource.java` |
| A | `java/hsm-bulk-client/src/test/java/com/hsm/client/deidjob/JsonRecordSourceTest.java` |
| A | `java/hsm-bulk-client/config-examples/deid-profiles/json-source-example.yml` |
| M | `java/hsm-bulk-client/src/main/java/com/hsm/client/deidjob/DeidJob.java` (open a JSON source; manifest and review use the columns seen) |
| M | `java/hsm-bulk-client/src/main/java/com/hsm/client/config/ClientProperties.java` (`source.type: JSON`, `source.json`) |
| M | `java/hsm-bulk-client/src/main/resources/application.yml` |
| M | `java/hsm-bulk-client/config-examples/deid-csv-export-example.yml` |
| M | `java/hsm-bulk-client/src/test/java/com/hsm/client/deidjob/DeidJobTest.java` |
| M | `java/hsm-crypto-client/src/main/java/com/hsm/client/deid/DeidProfile.java` (`outputName` leaves out `[]`) |
| M | `java/docs/RUNBOOK.md` |

Notes on phase 3 as built:

- **No new dependency.** Jackson (already used by `hsm-crypto-client`) reads
  the file as a stream: line by line for `LINES`, element by element for
  `ARRAY`. Duplicate keys are rejected; decimals keep their scale.
- **Paths not seen don't fail validation** (section 9.1): a field that's
  absent from the first records may still be present later, so failing on it
  would make runs depend on the data's order.

### Design

| | Path |
|---|---|
| M | `java/docs/DEIDENTIFICATION.md` (this document) |

## 15. Possible later

None of these is planned. Each can be added without changing the methods or
the profile format.

| Item | Note |
|---|---|
| Delivery by the job itself (PGP encrypt + SFTP) | Only if the existing transfer process can't handle it |
| Vendor registry (BAA expiry checks, delivery details) | Useful once there are many vendors |
| Forget one member (`--deid.forget --scope <s> --domain member --value <id>`) | Computes the lookup hash and deletes that member's surrogate row (section 16) |
| Files coming back from vendors | Map surrogates or tokens back to member IDs |
| JSON Lines or Parquet output | Output is CSV only for now |
| Reversible pseudonymization, FF1 format-preserving | Only if a vendor needs it |
| Spark adapter / Databricks functions | Reuse `com.hsm.client.deid`; Python must pass the test vectors |
| Unstructured files | A separate effort (PII detection, OCR, review) |

## 16. Decisions and open questions

| # | Question | Status |
|---|---|---|
| 1 | **Vendors:** one or many? | **Decided: any number.** One profile per vendor, one Job per profile (section 11). Each vendor's basis (`minimum-necessary` or `safe-harbor`) is chosen when that vendor is onboarded. |
| 2 | **Approval:** who approves profiles? | **Decided: the onboarding team.** The job runs on the client side, in the data owner's namespace, so the data owner's onboarding team owns each profile and its approval (with privacy sign-off as their process requires). **Still open:** should production refuse a profile whose SHA-256 isn't on an approved list? |
| 3 | **Retention:** how long are a vendor's surrogate rows kept? | **Decided: as long as the member is needed.** A member's surrogate row is kept while that member is still exported or may still be asked about. See "Surrogate retention" below. |
| 4 | **Hand-off:** who moves the file to the vendor, push or pull? | **Decided: push, set up by the client.** `hsm-bulk-client` runs in the client's scope, so the client sets up the MFT, its pick-up folder and the vendor target. See "Hand-off" below. |

### Surrogate retention

The `deid_surrogate` rows are the **only** link between a surrogate the
vendor holds (`S-8F2KQ-91QDM`) and our member (`M1234567`).

- **While rows are kept:** the same member gets the same surrogate every
  run, so the vendor can follow one member across monthly files. We can
  also map a vendor's question ("what about S-8F2KQ-91QDM?") back to the
  member.
- **Rows are never altered.** Changing a surrogate would make the vendor
  see the same member as two people. A surrogate changes only by deleting
  the scope's rows on purpose, which starts a new, unlinkable series.
- **When rows are deleted:** every file the vendor already holds becomes
  permanently anonymous to us, and the next run issues new surrogates.

**Decision: keep a member's surrogate for as long as the member is needed.**

- A row stays while the member is still in the vendor's files, or the
  vendor may still ask about them. The job never deletes or changes rows.
- **When a member is no longer needed** (and no open question from the
  vendor refers to them), the client may delete that member's rows. This is
  an operator step, recorded in the RUNBOOK:
  - one member: delete the rows whose `lookup_hash` matches the member.
    The table holds no plain member ID, so the hash must be computed with
    the surrogate-table key; a small command for this is listed in
    section 15 and isn't in the first phases;
  - a whole vendor, when its relationship ends:
    `DELETE FROM deid_surrogate WHERE scope = ?`.
- **A member who returns after deletion** gets a new surrogate, which the
  vendor can't link to the old one. That is the intended effect.
- Starting a vendor afresh without deleting: use a new scope name (e.g.
  `zen-analytics-2`).

The design adds no automatic expiry.

### Hand-off

**Decision: push, set up by the client.** `hsm-bulk-client` runs in the
client's own scope (namespace and storage), so the client owns delivery:

```
hsm-bulk-client (DEID Job)  ──►  outbound folder  ──►  client's MFT  ──►  vendor target
   writes .part, renames          (client's ADLS          (client sets          (SFTP etc.,
   to .csv when complete           or volume)              up schedule)          set by client)
```

| Who | Does |
|---|---|
| **The job** | Writes `<file>.csv` and `<file>.manifest.json` to `client.deid.output.root`, one folder per vendor profile. Never connects to a vendor. |
| **The client** | Sets up the MFT: the pick-up folder (the same as `output.root`), the vendor target, the schedule, and archiving or deleting files after sending. |
| **The vendor** | Receives the file. Gets no access to the client's storage. |

- The MFT must pick up only `*.csv`, never `*.part`, so it never sends a
  half-written file. The manifest stays behind (section 10.2).
- A separate "done" marker file isn't needed: the rename happens only
  when the file is complete. One can be added if a client's MFT requires
  it.
