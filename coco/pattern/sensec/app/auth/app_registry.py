"""
App registry — maps app_id to permitted scopes.

Backed by the same Postgres DB as the EDEK store so registrations
survive restarts. An in-process LRU cache keeps the hot path fast.
"""

from __future__ import annotations

from functools import lru_cache

from sqlalchemy import String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class _Base(DeclarativeBase):
    pass


class AppRegistration(_Base):
    __tablename__ = "app_registrations"

    app_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    allowed_scopes: Mapped[str] = mapped_column(String(512))   # comma-separated
    description: Mapped[str] = mapped_column(String(512), default="")
    active: Mapped[bool] = mapped_column(default=True)


class AppDecryptGrant(_Base):
    """
    Authorizes grantee_app_id to decrypt EDEK records owned by owner_app_id.
    Without a matching row here, an app may only decrypt data it encrypted
    itself — cross-app decrypt is denied by default.
    """
    __tablename__ = "app_decrypt_grants"

    grantee_app_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    owner_app_id: Mapped[str] = mapped_column(String(128), primary_key=True)


class AppRegistryError(Exception):
    pass


class AppRegistry:
    def __init__(self, session_factory) -> None:
        self._session_factory = session_factory
        self._cache: dict[str, list[str]] = {}
        self._grant_cache: dict[tuple[str, str], bool] = {}

    async def get_scopes(self, app_id: str) -> list[str]:
        if app_id in self._cache:
            return self._cache[app_id]
        async with self._session_factory() as session:
            row = await session.get(AppRegistration, app_id)
            if row is None or not row.active:
                raise AppRegistryError(f"Unknown or inactive app_id: {app_id}")
            scopes = [s.strip() for s in row.allowed_scopes.split(",")]
            self._cache[app_id] = scopes
            return scopes

    def invalidate(self, app_id: str) -> None:
        self._cache.pop(app_id, None)

    async def set_active(self, app_id: str, active: bool) -> None:
        """
        Block or restore an app. Must invalidate the scope cache in the same
        operation — without this, a deactivated app's already-cached scopes
        keep working until something else happens to evict them (e.g. a
        process restart), silently defeating the block.
        """
        async with self._session_factory() as session:
            row = await session.get(AppRegistration, app_id)
            if row is None:
                raise AppRegistryError(f"Unknown app_id: {app_id}")
            row.active = active
            await session.commit()
        self.invalidate(app_id)

    async def require_scope(self, app_id: str, scope: str) -> None:
        scopes = await self.get_scopes(app_id)
        if scope not in scopes:
            raise AppRegistryError(f"app_id={app_id} is not permitted to: {scope}")

    async def is_granted(self, grantee_app_id: str, owner_app_id: str) -> bool:
        """True if grantee_app_id may decrypt data owned by owner_app_id."""
        if grantee_app_id == owner_app_id:
            return True
        key = (grantee_app_id, owner_app_id)
        if key in self._grant_cache:
            return self._grant_cache[key]
        async with self._session_factory() as session:
            row = await session.get(AppDecryptGrant, key)
            granted = row is not None
            self._grant_cache[key] = granted
            return granted

    async def add_grant(self, grantee_app_id: str, owner_app_id: str) -> None:
        async with self._session_factory() as session:
            existing = await session.get(AppDecryptGrant, (grantee_app_id, owner_app_id))
            if existing is None:
                session.add(AppDecryptGrant(grantee_app_id=grantee_app_id, owner_app_id=owner_app_id))
                await session.commit()
        self._grant_cache[(grantee_app_id, owner_app_id)] = True

    async def remove_grant(self, grantee_app_id: str, owner_app_id: str) -> None:
        async with self._session_factory() as session:
            existing = await session.get(AppDecryptGrant, (grantee_app_id, owner_app_id))
            if existing is not None:
                await session.delete(existing)
                await session.commit()
        self._grant_cache.pop((grantee_app_id, owner_app_id), None)

    async def list_grants(self) -> list[dict[str, str]]:
        async with self._session_factory() as session:
            rows = (await session.scalars(select(AppDecryptGrant))).all()
            return [{"grantee_app_id": r.grantee_app_id, "owner_app_id": r.owner_app_id} for r in rows]
