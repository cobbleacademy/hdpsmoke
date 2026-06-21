from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.audit.logger import audit_log
from app.dependencies import AuthenticatedCaller, get_caller, get_db_session, get_kek_client
from app.models.schemas import EncryptRequest, EncryptResponse
from app.services import encryption_service

router = APIRouter(prefix="/encrypt", tags=["encryption"])


@router.post("", response_model=EncryptResponse, status_code=201)
async def encrypt_endpoint(
    body: EncryptRequest,
    request: Request,
    caller: Annotated[AuthenticatedCaller, Depends(get_caller)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    kek_client=Depends(get_kek_client),
):
    if "encrypt" not in caller.scopes:
        audit_log("encrypt", app_id=caller.app_id, sub=caller.sub,
                   caller_ip=request.client.host if request.client else "",
                   status="failure", reason="scope_not_permitted")
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Scope 'encrypt' not permitted")

    return await encryption_service.encrypt(
        request=body,
        app_id=caller.app_id,
        caller_sub=caller.sub,
        kek_client=kek_client,
        session=session,
        caller_ip=request.client.host if request.client else "",
    )
