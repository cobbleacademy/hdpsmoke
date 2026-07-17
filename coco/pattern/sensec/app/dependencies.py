"""
FastAPI dependency providers — all singletons initialised at startup.
Import `get_*` functions and use them via `Depends()`.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Annotated, AsyncGenerator

_log = logging.getLogger(__name__)

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.audit.logger import audit_log
from app.auth.app_registry import AppRegistration, AppRegistry, AppRegistryError
from app.auth.jwt_validator import JWTValidator, TokenValidationError
from app.config import Settings, get_settings
from app.crypto.dek_cache import DEKCache, NullDEKCache
from app.crypto.kek_client import KEKClient
from app.models.edek_record import Base as EDEKBase


# ── Singletons (set in main.py lifespan) ─────────────────────────────────────

_kek_client: KEKClient | None = None
_jwt_validator: object | None = None
_app_registry: AppRegistry | None = None
_session_factory: async_sessionmaker | None = None
_dek_cache: DEKCache | NullDEKCache = NullDEKCache()


async def init_dependencies(settings: Settings) -> None:
    global _kek_client, _jwt_validator, _app_registry, _session_factory, _dek_cache

    if settings.demo_mode:
        from app.auth.app_registry import AppDecryptGrant
        from app.demo.consumer_store import ConsumerBase
        from app.demo.mock_jwt_validator import DEMO_GRANTS, DEMO_SCOPES, MockJWTValidator
        from app.demo.mock_kek_client import MockKEKClient

        engine = create_async_engine(settings.demo_database_url)
        _session_factory = async_sessionmaker(engine, expire_on_commit=False)

        async with engine.begin() as conn:
            await conn.run_sync(EDEKBase.metadata.create_all)
            await conn.run_sync(AppRegistration.metadata.create_all)
            await conn.run_sync(ConsumerBase.metadata.create_all)

        async with _session_factory() as session:
            for app_id, scopes in DEMO_SCOPES.items():
                existing = await session.get(AppRegistration, app_id)
                if existing is None:
                    session.add(AppRegistration(
                        app_id=app_id,
                        allowed_scopes=",".join(scopes),
                        description="Seeded demo app",
                        active=True,
                    ))
            for grantee_app_id, owner_app_id in DEMO_GRANTS:
                existing = await session.get(AppDecryptGrant, (grantee_app_id, owner_app_id))
                if existing is None:
                    session.add(AppDecryptGrant(grantee_app_id=grantee_app_id, owner_app_id=owner_app_id))
            await session.commit()

        _kek_client = MockKEKClient()
        _jwt_validator = MockJWTValidator()
        _app_registry = AppRegistry(_session_factory)
        return

    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    _session_factory = async_sessionmaker(engine, expire_on_commit=False)
    _kek_client = KEKClient(settings)
    _jwt_validator = JWTValidator(settings)
    _app_registry = AppRegistry(_session_factory)

    if settings.dek_cache_enabled and settings.redis_url:
        import base64
        import redis.asyncio as aioredis

        # Load current CEK version pointer (e.g. "v2") then the keyed secret.
        # A previous version (e.g. "v1") is also loaded as the fallback so
        # entries written before rotation are still readable during the 30s
        # convergence window — no pod restart needed.
        current_ver = await _kek_client.fetch_secret(settings.dek_cache_key_current_secret_name)
        current_ver = current_ver.strip()
        cek = base64.b64decode(
            await _kek_client.fetch_secret(f"hsm-dek-cache-key-{current_ver}")
        )

        prev_cek: bytes | None = None
        prev_ver: str | None = None
        try:
            ver_num = int(current_ver.lstrip("v"))
            if ver_num > 1:
                prev_ver = f"v{ver_num - 1}"
                prev_cek = base64.b64decode(
                    await _kek_client.fetch_secret(f"hsm-dek-cache-key-{prev_ver}")
                )
        except Exception:
            pass  # first version or non-numeric scheme — no prev CEK

        redis_client = aioredis.from_url(
            settings.redis_url,
            decode_responses=False,
            socket_connect_timeout=2,
            socket_timeout=1,
        )
        excluded = {c.strip().lower() for c in settings.dek_cache_excluded_classifications.split(",") if c.strip()}
        _dek_cache = DEKCache(
            redis_client=redis_client,
            cek=cek,
            version=current_ver,
            ttl_seconds=settings.dek_cache_ttl_seconds,
            excluded_classifications=excluded,
            prev_cek=prev_cek,
            prev_version=prev_ver,
        )

        # Background task: poll Azure KV every dek_cache_reload_interval_seconds.
        # When a new version is detected, DEKCache.rotate() is called in-process —
        # no pod restart, no MISS storm, no cross-pod collisions.
        asyncio.create_task(
            _cek_reload_loop(_dek_cache, _kek_client, settings),
            name="cek-reload",
        )


# ── CEK hot-reload background task ───────────────────────────────────────────

async def _cek_reload_loop(
    cache: DEKCache,
    kek_client: KEKClient,
    settings,
) -> None:
    """
    Polls Azure KV every dek_cache_reload_interval_seconds for a new CEK
    version.  On change, calls cache.rotate() in-process — no pod restart
    required.  All pods converge within one poll interval (~30s), well within
    the 60s TTL, so no MISS storm and no cross-pod key collisions occur.
    """
    import base64
    while True:
        await asyncio.sleep(settings.dek_cache_reload_interval_seconds)
        try:
            latest_ver = (
                await kek_client.fetch_secret(settings.dek_cache_key_current_secret_name)
            ).strip()
            if latest_ver != cache.current_version:
                new_cek = base64.b64decode(
                    await kek_client.fetch_secret(f"hsm-dek-cache-key-{latest_ver}")
                )
                cache.rotate(new_cek, latest_ver)
                _log.info(
                    "CEK rotated",
                    extra={"prev_version": cache.current_version, "new_version": latest_ver},
                )
        except Exception as exc:
            # Log and continue — a failed poll leaves the current CEK in place.
            # The next poll interval will retry.
            _log.warning("CEK reload poll failed: %s", exc)


# ── Dependency functions ──────────────────────────────────────────────────────

def get_dek_cache() -> DEKCache | NullDEKCache:
    return _dek_cache


def get_kek_client() -> KEKClient:
    assert _kek_client is not None, "KEKClient not initialised"
    return _kek_client


def get_app_registry() -> AppRegistry:
    assert _app_registry is not None, "AppRegistry not initialised"
    return _app_registry


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    assert _session_factory is not None, "DB not initialised"
    async with _session_factory() as session:
        yield session


class AuthenticatedCaller:
    """Resolved once per request; carries validated identity."""
    def __init__(self, app_id: str, sub: str, scopes: list[str]) -> None:
        self.app_id = app_id
        self.sub = sub
        self.scopes = scopes


async def get_caller(
    request: Request,
    authorization: Annotated[str, Header()],
    x_app_id: Annotated[str, Header(alias="X-App-ID")],
    registry: Annotated[AppRegistry, Depends(get_app_registry)],
) -> AuthenticatedCaller:
    caller_ip = request.client.host if request.client else ""

    if not authorization.startswith("Bearer "):
        audit_log("auth_failure", app_id=x_app_id, caller_ip=caller_ip,
                   status="failure", reason="missing_bearer_token")
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Bearer token required")
    token = authorization.removeprefix("Bearer ")

    assert _jwt_validator is not None
    try:
        claims = _jwt_validator.validate(token)
    except TokenValidationError as exc:
        audit_log("auth_failure", app_id=x_app_id, caller_ip=caller_ip,
                   status="failure", reason=f"invalid_token: {exc}")
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, str(exc))

    if claims.get("app_id") != x_app_id:
        audit_log("auth_failure", app_id=x_app_id, caller_ip=caller_ip,
                   status="failure", reason="app_id_claim_mismatch",
                   token_app_id=claims.get("app_id"))
        raise HTTPException(status.HTTP_403_FORBIDDEN, "app_id claim does not match X-App-ID header")

    try:
        scopes = await registry.get_scopes(x_app_id)
    except AppRegistryError as exc:
        audit_log("auth_failure", app_id=x_app_id, caller_ip=caller_ip,
                   status="failure", reason=f"unknown_or_inactive_app: {exc}")
        raise HTTPException(status.HTTP_403_FORBIDDEN, str(exc))

    return AuthenticatedCaller(app_id=x_app_id, sub=claims.get("sub", ""), scopes=scopes)
