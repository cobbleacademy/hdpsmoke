# DEK Cache TTL & CEK Rotation Interval

Two independent knobs govern the Redis DEK-cache layer, and conflating them
is the most common mistake:

| Knob | Config | Default | What it actually controls |
|---|---|---|---|
| **DEK cache TTL** | `DEK_CACHE_TTL_SECONDS` | 60s | How long an already-unwrapped DEK stays in Redis before the next `/decrypt` for that `edek_id` must go back to Azure Managed HSM |
| **CEK rotation interval** | `ROTATION_INTERVAL_HOURS` (`cek-rotation-service`) | 4h | How often the *key that encrypts the Redis cache entries themselves* (the CEK) gets rotated — a security-hygiene control, not a cache-performance control |

The key fact that shapes everything below: **the cache is keyed per
`edek_id`** (`{slot}:{kv_version}:{edek_id}`), not per app_id and not per
request. TTL only pays off when the *same* `edek_id` is decrypted more than
once within the TTL window. That single fact explains every scenario below.

(Not covered here: `KEK_ROTATION_CRON`, the monthly master-key re-wrap
schedule, and `PBAC_CACHE_TTL_SECONDS`, the unrelated PlainID decision
cache — both are separate mechanisms from the DEK cache TTL discussed here.)

## How TTL helps under high concurrency

High concurrency against a **hot/repeated working set** (many requests
concentrated on a relatively small number of records — e.g., an app
repeatedly re-decrypting the same customer record for every page view) is
exactly the case the cache was designed for:

- First request for an `edek_id` → HSM unwrap (the expensive, rate-limited
  path: network round-trip + RSA-OAEP-256 unwrap against Managed HSM).
- Every subsequent request for the same `edek_id` within the TTL window →
  Redis `GET` (~1ms), zero HSM calls.
- Net effect: HSM call volume collapses from O(requests) to roughly
  O(unique hot keys / TTL window) — the difference between saturating the
  HSM's own throughput ceiling and staying comfortably under it. This is the
  primary reason the cache exists.

## How TTL helps (or doesn't) under bulk enc/dec

Bulk workloads (data migration, nightly re-encrypt jobs, backfills) are the
opposite case: each record typically has its **own unique DEK**, generated
fresh by `/encrypt` (`DEK = random_bytes(32)` per call). A bulk pass over N
distinct records touches N distinct `edek_id`s exactly once each.

- **TTL provides essentially no benefit here** — there's no repeat access to
  amortize. Every unique key still costs exactly one real HSM unwrap,
  regardless of whether TTL is 60s or 6000s.
- The actual bottleneck for bulk throughput is **Azure Managed HSM's own
  RPS/concurrency ceiling**, not cache configuration. The lever that matters
  is parallelism (how many concurrent unwrap calls are driven) tuned to stay
  under that ceiling — a Redis TTL change does nothing for this.
- The one exception: if the bulk job re-touches the same rows (retries,
  overlapping range re-runs), TTL helps *those* repeat touches within the
  window — but that's incidental, not the primary design target.

## Few app_ids, high concurrency vs. few app_ids, bulk

This is really the same distinction restated along a different axis — app_id
count isn't actually the variable that matters, key-reuse pattern is:

| | Few app_ids, high concurrency (hot keys) | Few app_ids, bulk (unique keys) |
|---|---|---|
| Cache hit ratio | High — same small working set gets hammered repeatedly | Near-zero — each key touched once |
| What TTL tuning buys | Real payoff: a longer TTL extends the hit window and further reduces HSM load, at the cost of a longer window where decrypted DEK bytes sit in Redis (CEK-encrypted, but still worth weighing) | Nothing — raising TTL doesn't change a one-shot access pattern |
| Real lever | TTL sizing (and possibly Redis capacity/eviction policy if the hot set is large) | HSM concurrency ceiling + parallel worker count, not cache config |
| Failure mode if misconfigured | TTL too short → cache thrashes, HSM gets hammered anyway despite hot keys | Expecting the cache to save you → false sense of headroom, HSM still gets the full N calls |

**Guidance for future tuning:** treat TTL as a workload-aware setting rather
than a single global value — a longer TTL suits the interactive/hot-key
path; bulk jobs are a separate concern entirely, sized against the HSM's
documented throughput limit rather than cache configuration.

## Where the CEK rotation interval fits (it mostly doesn't, by design)

`ROTATION_INTERVAL_HOURS` is **decoupled from all of the above** — that
decoupling is deliberate, not an oversight:

- CEK rotation swaps which Key Vault secret slot (`alpha`/`beta`) encrypts
  *new* cache writes. It never forces existing cache entries to expire
  early — the "prev slot fallback" read path means entries under the
  outgoing slot stay readable until their own TTL naturally expires.
- A rotation event therefore causes **zero cache-miss storm** and **zero HSM
  load spike** — it's transparent to both the high-concurrency and bulk
  scenarios above.
- Its only real effect is security: shortening the interval reduces the
  blast radius if a CEK is ever compromised (limits how much historical
  cached-DEK data one leaked CEK could decrypt). It's a security/exposure
  dial, not a performance dial — tuning it to "help under load" would be a
  no-op for throughput and only add operational churn.
