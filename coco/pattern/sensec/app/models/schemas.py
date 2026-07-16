from __future__ import annotations

import uuid
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator
import base64


class EncryptRequest(BaseModel):
    plaintext: Annotated[str, Field(min_length=1, max_length=1_048_576)]  # 1 MiB max
    encoding: Literal["utf8", "base64"] = "utf8"   # how to interpret plaintext on decrypt's way back out
    data_classification: str | None = None         # e.g. "pii", "pci" — drives audit/retention queries, never enforced here
    end_user_id: str | None = None                 # logged-in user who triggered the call; passed by client for SIEM audit trail
    context: dict[str, str] = Field(default_factory=dict)                 # caller metadata, stored in audit log only


class EncryptResponse(BaseModel):
    edek_id: uuid.UUID
    owner_app_id: str   # the app_id bound into the AAD for this ciphertext
    algorithm: str
    encoding: str
    iv_b64: str
    ciphertext_b64: str
    tag_b64: str
    kek_version: str


class DecryptRequest(BaseModel):
    edek_id: uuid.UUID
    iv_b64: str
    ciphertext_b64: str
    tag_b64: str
    end_user_id: str | None = None                 # logged-in user who triggered the call; passed by client for SIEM audit trail

    @field_validator("iv_b64", "ciphertext_b64", "tag_b64", mode="before")
    @classmethod
    def _validate_base64(cls, v: str) -> str:
        try:
            base64.b64decode(v, validate=True)
        except Exception:
            raise ValueError("field must be valid base64")
        return v


class DecryptResponse(BaseModel):
    plaintext: str
    owner_app_id: str   # the app_id used as AAD when this record was encrypted
    algorithm: str
    encoding: str        # tells the caller how to interpret plaintext (utf8 vs base64)


class RotateKEKResponse(BaseModel):
    new_kek_version: str
    records_queued: int


class HealthResponse(BaseModel):
    status: str
    vault_reachable: bool
    db_reachable: bool


class AppRegistration(BaseModel):
    app_id: str
    allowed_scopes: list[str]   # e.g. ["encrypt", "decrypt"]
    description: str = ""


class GrantRequest(BaseModel):
    grantee_app_id: str   # the app being granted read access
    owner_app_id: str      # the app whose encrypted data may be read


class GrantResponse(BaseModel):
    grantee_app_id: str
    owner_app_id: str


class GrantListResponse(BaseModel):
    grants: list[GrantResponse]


class AppStatusRequest(BaseModel):
    app_id: str
    active: bool


class AppStatusResponse(BaseModel):
    app_id: str
    active: bool


class ConsumerAccountCreateRequest(BaseModel):
    customer_name: Annotated[str, Field(min_length=1, max_length=128)]
    email: Annotated[str, Field(min_length=1, max_length=256)]
    account_number: Annotated[str, Field(min_length=1, max_length=64)]   # sensitive — never stored as-is


class ConsumerAccountResponse(BaseModel):
    id: int
    customer_name: str          # non-sensitive
    email: str                  # non-sensitive
    account_number_ciphertext_preview: str   # sensitive column, truncated — never the full blob, never plaintext
    edek_id: uuid.UUID          # required to decrypt — fixed-size reference, safe to show in full
    iv_b64: str                 # required to decrypt — fixed 16 chars, safe to show in full
    tag_b64: str                # required to decrypt — fixed 24 chars, safe to show in full
    created_at: str


class ConsumerRevealRequest(BaseModel):
    reveal_as: str              # which app_id is asking to decrypt — exercises the same grant model as /decrypt
    end_user_id: str | None = None   # logged-in user who triggered the reveal; passed through to audit log


class ConsumerRevealResponse(BaseModel):
    id: int
    account_number: str   # decrypted on demand, never written back to the table
