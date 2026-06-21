from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from app.audit.logger import audit_log
from app.auth.app_registry import AppRegistryError
from app.dependencies import AuthenticatedCaller, get_app_registry, get_caller, get_kek_client
from app.models.schemas import (
    AppStatusRequest,
    AppStatusResponse,
    GrantListResponse,
    GrantRequest,
    GrantResponse,
    HealthResponse,
    RotateKEKResponse,
)
from app.services import rotation_service

router = APIRouter(prefix="/admin", tags=["admin"])


def _deny(event: str, caller: AuthenticatedCaller, scope: str, **extra) -> None:
    audit_log(event, app_id=caller.app_id, sub=caller.sub,
              status="failure", reason=f"scope_not_permitted:{scope}", **extra)


@router.post("/rotate-kek", response_model=RotateKEKResponse)
async def rotate_kek_endpoint(
    caller: Annotated[AuthenticatedCaller, Depends(get_caller)],
    kek_client=Depends(get_kek_client),
):
    if "rotate" not in caller.scopes:
        _deny("kek_rotation_denied", caller, "rotate")
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Scope 'rotate' not permitted")

    # Demo HSM stand-in must mint a new key version itself; Azure does this
    # via its own rotation policy, so the real client has no such method.
    if hasattr(kek_client, "rotate_to_new_version"):
        await kek_client.rotate_to_new_version()

    # session_factory injected via module-level reference in rotation_service
    from app.dependencies import _session_factory
    assert _session_factory is not None
    return await rotation_service.rotate_kek(
        kek_client=kek_client,
        session_factory=_session_factory,
        triggered_by=f"api:{caller.sub}",
    )


@router.get("/health", response_model=HealthResponse)
async def health_endpoint(kek_client=Depends(get_kek_client)):
    vault_ok = False
    db_ok = False

    try:
        await kek_client.get_current_kek_version()
        vault_ok = True
    except Exception:
        pass

    try:
        from app.dependencies import _session_factory
        if _session_factory:
            async with _session_factory() as s:
                await s.execute(__import__("sqlalchemy").text("SELECT 1"))
            db_ok = True
    except Exception:
        pass

    overall = "ok" if (vault_ok and db_ok) else "degraded"
    return HealthResponse(status=overall, vault_reachable=vault_ok, db_reachable=db_ok)


@router.post("/grants", response_model=GrantResponse, status_code=201)
async def add_grant_endpoint(
    body: GrantRequest,
    caller: Annotated[AuthenticatedCaller, Depends(get_caller)],
    app_registry=Depends(get_app_registry),
):
    if "grant" not in caller.scopes:
        _deny("grant_added", caller, "grant", grantee_app_id=body.grantee_app_id, owner_app_id=body.owner_app_id)
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Scope 'grant' not permitted")

    await app_registry.add_grant(body.grantee_app_id, body.owner_app_id)
    audit_log(
        "grant_added",
        app_id=caller.app_id,
        sub=caller.sub,
        grantee_app_id=body.grantee_app_id,
        owner_app_id=body.owner_app_id,
        status="success",
    )
    return GrantResponse(grantee_app_id=body.grantee_app_id, owner_app_id=body.owner_app_id)


@router.delete("/grants", status_code=204)
async def remove_grant_endpoint(
    body: GrantRequest,
    caller: Annotated[AuthenticatedCaller, Depends(get_caller)],
    app_registry=Depends(get_app_registry),
):
    if "grant" not in caller.scopes:
        _deny("grant_removed", caller, "grant", grantee_app_id=body.grantee_app_id, owner_app_id=body.owner_app_id)
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Scope 'grant' not permitted")

    await app_registry.remove_grant(body.grantee_app_id, body.owner_app_id)
    audit_log(
        "grant_removed",
        app_id=caller.app_id,
        sub=caller.sub,
        grantee_app_id=body.grantee_app_id,
        owner_app_id=body.owner_app_id,
        status="success",
    )


@router.get("/grants", response_model=GrantListResponse)
async def list_grants_endpoint(
    caller: Annotated[AuthenticatedCaller, Depends(get_caller)],
    app_registry=Depends(get_app_registry),
):
    if "grant" not in caller.scopes:
        _deny("grants_listed", caller, "grant")
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Scope 'grant' not permitted")

    grants = await app_registry.list_grants()
    return GrantListResponse(grants=[GrantResponse(**g) for g in grants])


@router.post("/apps/status", response_model=AppStatusResponse)
async def set_app_status_endpoint(
    body: AppStatusRequest,
    caller: Annotated[AuthenticatedCaller, Depends(get_caller)],
    app_registry=Depends(get_app_registry),
):
    """
    Block or restore an app. This is intentionally a separate scope from
    'grant' — granting a decrypt relationship and disabling another app's
    ability to act entirely are different powers, and an incident response
    workflow shouldn't need to hold both by default.
    """
    if "manage_apps" not in caller.scopes:
        _deny("app_status_changed", caller, "manage_apps",
              target_app_id=body.app_id, requested_active=body.active)
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Scope 'manage_apps' not permitted")

    try:
        await app_registry.set_active(body.app_id, body.active)
    except AppRegistryError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, str(exc))

    audit_log(
        "app_status_changed",
        app_id=caller.app_id,
        sub=caller.sub,
        target_app_id=body.app_id,
        active=body.active,
        status="success",
    )
    return AppStatusResponse(app_id=body.app_id, active=body.active)
