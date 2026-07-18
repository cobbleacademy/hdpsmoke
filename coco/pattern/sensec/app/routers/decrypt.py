from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.audit.logger import audit_log
from app.dependencies import AuthenticatedCaller, get_app_registry, get_caller, get_db_session, get_dek_cache, get_kek_client, get_pbac_client
from app.models.schemas import DecryptRequest, DecryptResponse
from app.services import decryption_service

router = APIRouter(prefix="/decrypt", tags=["encryption"])


@router.post("", response_model=DecryptResponse)
async def decrypt_endpoint(
    body: DecryptRequest,
    request: Request,
    caller: Annotated[AuthenticatedCaller, Depends(get_caller)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    kek_client=Depends(get_kek_client),
    app_registry=Depends(get_app_registry),
    dek_cache=Depends(get_dek_cache),
    pbac_client=Depends(get_pbac_client),
):
    if "decrypt" not in caller.scopes:
        audit_log("decrypt", app_id=caller.app_id, sub=caller.sub,
                   edek_id=str(body.edek_id),
                   caller_ip=request.client.host if request.client else "",
                   status="failure", reason="scope_not_permitted")
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Scope 'decrypt' not permitted")

    return await decryption_service.decrypt(
        request=body,
        app_id=caller.app_id,
        caller_sub=caller.sub,
        caller_scopes=caller.scopes,
        kek_client=kek_client,
        session=session,
        app_registry=app_registry,
        caller_ip=request.client.host if request.client else "",
        dek_cache=dek_cache,
        pbac_client=pbac_client,
    )
