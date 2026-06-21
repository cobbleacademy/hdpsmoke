"""
Demo-mode dependency wiring.

When `DEMO_MODE=true`, the service runs with in-memory fakes standing in for
Azure Key Vault, Postgres, and JWT validation — the same shape of fakes used
by tests/integration/test_encrypt_decrypt.py — so the API (and the bundled
demo UI) works end-to-end with zero external infrastructure.
"""

from __future__ import annotations

import os

from unittest.mock import AsyncMock, MagicMock

from app import dependencies

FAKE_KEK_VERSION = "demo-v1"
DEMO_TOKEN = "demo.jwt.token"
DEMO_APP_ID = "app-demo"


def init_demo_dependencies() -> None:
    dek_key = os.urandom(32)
    edek_key = os.urandom(64)

    kek = MagicMock()
    kek.wrap_dek = AsyncMock(return_value=(edek_key, FAKE_KEK_VERSION))
    kek.unwrap_dek = AsyncMock(return_value=dek_key)
    kek.get_current_kek_version = AsyncMock(return_value=FAKE_KEK_VERSION)
    kek.close = AsyncMock()
    dependencies._kek_client = kek

    store: dict[str, object] = {}

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc):
            return None

        def add(self, obj) -> None:
            store[str(obj.edek_id)] = obj

        async def commit(self) -> None:
            pass

        async def get(self, _model, pk):
            return store.get(str(pk))

        async def execute(self, _stmt):
            return None

    class FakeSessionFactory:
        def __call__(self) -> FakeSession:
            return FakeSession()

    dependencies._session_factory = FakeSessionFactory()

    registry = MagicMock()
    registry.get_scopes = AsyncMock(return_value=["encrypt", "decrypt", "rotate"])
    registry.require_scope = AsyncMock()
    dependencies._app_registry = registry

    validator = MagicMock()
    validator.validate = MagicMock(
        side_effect=lambda token: {"sub": "demo-user", "app_id": DEMO_APP_ID}
        if token == DEMO_TOKEN
        else _reject(token)
    )
    dependencies._jwt_validator = validator


def _reject(_token: str):
    from app.auth.jwt_validator import TokenValidationError

    raise TokenValidationError("invalid demo token")
