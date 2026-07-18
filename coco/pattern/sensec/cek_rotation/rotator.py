"""
CEK rotation logic.

Generates a fresh 32-byte Content Encryption Key, writes it to the
*inactive* slot in Azure Key Vault, then flips the current_key pointer
to that slot so the main HSM service pods pick it up on their next poll.

The AKV secret ID has the form:
  https://{vault}.vault.azure.net/secrets/{name}/{version}
The last path segment is the kv_version the main service records in its
EDEK metadata so it can always re-derive which key generation to use.

Post-rotation Redis ops (optional, controlled by config.redis_post_rotation_mode):
  - count  : log entry counts per CEK version before and after migration
  - rekey  : re-encrypt existing cache entries under the new CEK in-place
  - flush  : delete all DEK cache entries (forces a re-warm from HSM)
"""

from __future__ import annotations

import base64
import os
import time
from datetime import datetime, timezone

import structlog

log = structlog.get_logger("cek_rotation.rotator")


async def rotate_cek(secret_client, config, redis_client=None) -> dict:
    """
    Perform one CEK rotation cycle.

    Parameters
    ----------
    secret_client:
        An ``azure.keyvault.secrets.aio.SecretClient`` already authenticated
        with the Rotation SPN (needs secrets/get + secrets/set on both CEK
        slot secrets and the current_key pointer secret).
    config:
        A ``cek_rotation.config.Settings`` instance.
    redis_client:
        Optional ``redis.asyncio.Redis`` client.  Required when
        ``config.redis_post_rotation_mode != "none"``; ignored otherwise.

    Returns
    -------
    dict with keys: slot, kv_version, rotated_at (ISO-8601), redis_ops (dict).
    """
    t0 = time.monotonic()

    # 1. Discover which slot is currently active.
    current_secret = await secret_client.get_secret(config.current_key_secret_name)
    active_slot = (current_secret.value or "").strip().lower()
    if active_slot not in ("alpha", "beta"):
        raise ValueError(
            f"current_key secret has unexpected value {active_slot!r}; "
            "expected 'alpha' or 'beta'"
        )

    # 2. The inactive slot becomes the write target.
    inactive_slot = "beta" if active_slot == "alpha" else "alpha"
    active_secret_name = (
        config.cek_alpha_secret_name
        if active_slot == "alpha"
        else config.cek_beta_secret_name
    )
    inactive_secret_name = (
        config.cek_alpha_secret_name
        if inactive_slot == "alpha"
        else config.cek_beta_secret_name
    )

    log.info(
        "cek_rotation_starting",
        active_slot=active_slot,
        target_slot=inactive_slot,
        target_secret=inactive_secret_name,
    )

    # 3. Read the OLD active CEK bytes before overwriting the inactive slot.
    #    Needed only when rekey mode is requested so we can decrypt existing
    #    Redis entries.  The Rotation SPN must hold secrets/get on both slot
    #    secrets for this to work.
    old_cek_bytes: bytes | None = None
    old_kv_version: str | None = None
    do_rekey = (
        redis_client is not None
        and config.redis_post_rotation_mode == "rekey"
    )
    if do_rekey:
        try:
            old_secret = await secret_client.get_secret(active_secret_name)
            old_cek_b64 = old_secret.value or ""
            old_kv_version = (old_secret.properties.id or "").rstrip("/").rsplit("/", 1)[-1]
            old_cek_bytes = base64.b64decode(old_cek_b64)
        except Exception as exc:
            # Rekey is best-effort — fall back to flush if we can't read old CEK.
            log.warning(
                "old_cek_read_failed_falling_back_to_flush",
                error=str(exc),
            )
            do_rekey = False

    # 4. Generate a fresh 256-bit CEK and base64-encode it for AKV storage.
    new_cek_bytes = os.urandom(32)
    new_cek_b64 = base64.b64encode(new_cek_bytes).decode("ascii")

    # 5. Write the new CEK to the inactive slot.
    set_result = await secret_client.set_secret(inactive_secret_name, new_cek_b64)

    # 6. Extract kv_version from the returned secret ID.
    #    ID format: https://{vault}.vault.azure.net/secrets/{name}/{version}
    secret_id: str = set_result.id or ""
    kv_version = secret_id.rstrip("/").split("/")[-1]

    log.info(
        "cek_slot_written",
        slot=inactive_slot,
        secret_name=inactive_secret_name,
        kv_version=kv_version,
    )

    # 7. Flip the pointer — main service pods pick this up within their 30 s poll.
    await secret_client.set_secret(config.current_key_secret_name, inactive_slot)

    # 8. Post-rotation Redis ops (best-effort — never let Redis errors fail rotation).
    redis_ops_result: dict = {}
    if redis_client is not None and config.redis_post_rotation_mode != "none":
        from cek_rotation.redis_ops import count_by_version, flush_dek_cache, rekey_dek_cache

        try:
            # Always count first so we have a before-snapshot in the logs.
            before_counts = await count_by_version(redis_client)
            log.info("redis_before_rotation", counts=before_counts)
            redis_ops_result["before_counts"] = before_counts

            old_version = f"{active_slot}:{old_kv_version}" if old_kv_version else None
            new_version = f"{inactive_slot}:{kv_version}"

            if do_rekey and old_cek_bytes is not None and old_version is not None:
                rekey_result = await rekey_dek_cache(
                    redis_client,
                    old_cek=old_cek_bytes,
                    new_cek=new_cek_bytes,
                    old_version=old_version,
                    new_version=new_version,
                    default_ttl=config.dek_cache_ttl_seconds,
                )
                redis_ops_result["rekey"] = rekey_result

            elif config.redis_post_rotation_mode == "flush":
                deleted = await flush_dek_cache(redis_client)
                redis_ops_result["flushed"] = deleted

            # Count again after migration to confirm old-version entries are zero.
            after_counts = await count_by_version(redis_client)
            log.info("redis_after_rotation", counts=after_counts)
            redis_ops_result["after_counts"] = after_counts

            old_remaining = after_counts.get(old_version, 0) if old_version else 0
            redis_ops_result["old_version_entries_remaining"] = old_remaining
            if old_remaining:
                log.warning(
                    "old_cek_entries_remain",
                    count=old_remaining,
                    old_version=old_version,
                )

        except Exception as exc:
            log.error("redis_post_rotation_ops_failed", error=str(exc), exc_info=True)
            redis_ops_result["error"] = str(exc)

    rotated_at = datetime.now(tz=timezone.utc).isoformat()
    elapsed_ms = int((time.monotonic() - t0) * 1000)

    log.info(
        "cek_rotation_complete",
        slot=inactive_slot,
        kv_version=kv_version,
        rotated_at=rotated_at,
        elapsed_ms=elapsed_ms,
        redis_ops=redis_ops_result,
    )

    return {
        "slot": inactive_slot,
        "kv_version": kv_version,
        "rotated_at": rotated_at,
        "redis_ops": redis_ops_result,
    }
