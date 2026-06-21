"""
HSM Encryption Service — FastAPI entrypoint.

Startup sequence:
  1. Load settings
  2. Fetch Splunk HEC token from Key Vault (if enabled) and override setting
  3. Initialise audit logger (stdout + Splunk batcher)
  4. Initialise DB engine, KEK client, JWT validator, app registry
  5. Start Splunk flush loop and KEK rotation scheduler
  6. Register routers
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app.audit.logger import (
    audit_log,
    init_audit,
    start_splunk_batcher,
    stop_splunk_batcher,
)
from app.config import get_settings
from app.dependencies import get_kek_client, init_dependencies
from app.routers import admin, decrypt, encrypt

log = structlog.get_logger("main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()

    # Fetch Splunk HEC token from Key Vault so it never lives in .env in production
    if settings.splunk_enabled and not settings.splunk_hec_token and not settings.demo_mode:
        from app.crypto.kek_client import KEKClient
        _bootstrap_kek = KEKClient(settings)
        try:
            token = await _bootstrap_kek.fetch_secret("splunk-hec-token")
            settings.splunk_hec_token = token
        finally:
            await _bootstrap_kek.close()

    init_audit(settings)
    await init_dependencies(settings)
    await start_splunk_batcher()

    scheduler = None
    if settings.kek_rotation_enabled and not settings.demo_mode:
        from scheduler.kek_rotation_job import start_rotation_scheduler
        from app.dependencies import _session_factory
        scheduler = start_rotation_scheduler(
            cron_expr=settings.kek_rotation_cron,
            kek_client=get_kek_client(),
            session_factory=_session_factory,
        )

    audit_log("service_started", env=settings.service_env)
    log.info("hsm_service_started", env=settings.service_env)

    yield

    if scheduler:
        scheduler.shutdown(wait=False)
    await stop_splunk_batcher()
    await get_kek_client().close()
    audit_log("service_stopped", env=settings.service_env)


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title="HSM Encryption Service",
        description="Centralised AES-256-GCM envelope encryption backed by Azure Key Vault Managed HSM",
        version="1.0.0",
        docs_url=None if settings.service_env == "production" else "/docs",
        redoc_url=None if settings.service_env == "production" else "/redoc",
        lifespan=lifespan,
    )

    prefix = settings.api_v1_prefix
    app.include_router(encrypt.router, prefix=prefix)
    app.include_router(decrypt.router, prefix=prefix)
    app.include_router(admin.router, prefix=prefix)

    if settings.demo_mode:
        from app.routers import demo as demo_router
        app.include_router(demo_router.router, prefix=prefix)

        static_dir = os.path.join(os.path.dirname(__file__), "static")
        app.mount("/", StaticFiles(directory=static_dir, html=True), name="ui")

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        log.error("unhandled_exception", path=request.url.path, error=str(exc))
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error"},
        )

    return app


app = create_app()
