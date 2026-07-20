from __future__ import annotations

import uuid
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator
import base64


_MAX_PLAINTEXT_CHARS = 1_048_576   # hard ceiling: 1 MiB characters
_SOFT_LIMIT_BYTES = 65_536         # default soft limit: 64 KiB UTF-8 bytes


class EncryptRequest(BaseModel):
    plaintext: Annotated[str, Field(min_length=1, max_length=_MAX_PLAINTEXT_CHARS)]
    encoding: Literal["utf8", "base64"] = "utf8"   # how to interpret plaintext on decrypt's way back out
    data_classification: str | None = None         # e.g. "pii", "pci" — drives audit/retention queries, never enforced here
    end_user_id: str | None = None                 # logged-in user who triggered the call; passed by client for SIEM audit trail
    context: dict[str, str] = Field(default_factory=dict)                 # caller metadata, stored in audit log only

    @field_validator("plaintext", mode="after")
    @classmethod
    def _check_byte_length(cls, v: str) -> str:
        byte_len = len(v.encode("utf-8"))
        if byte_len > _MAX_PLAINTEXT_CHARS:
            raise ValueError(
                f"plaintext exceeds maximum size: {byte_len} bytes "
                f"(hard limit {_MAX_PLAINTEXT_CHARS} bytes)"
            )
        return v


class EncryptResponse(BaseModel):
    # ── Preferred: single opaque token, store and echo back as-is ────────────
    ciphertext_token: str           # "v1.<base64url(version|edek_id|iv|tag|ciphertext)>"

    # ── Informational fields — useful for logging/audit, not needed for decrypt
    edek_id: uuid.UUID
    owner_app_id: str
    algorithm: str
    encoding: str
    kek_version: str

    # ── Deprecated: individual binary fields, kept for backward compatibility ─
    # Clients should use ciphertext_token instead. These will be removed in v2.
    iv_b64: str
    ciphertext_b64: str
    tag_b64: str


class DecryptRequest(BaseModel):
    # ── Preferred path: single token ─────────────────────────────────────────
    ciphertext_token: str | None = None

    # ── Legacy path: individual fields (deprecated, will be removed in v2) ───
    edek_id: uuid.UUID | None = None
    iv_b64: str | None = None
    ciphertext_b64: str | None = None
    tag_b64: str | None = None

    end_user_id: str | None = None

    @field_validator("iv_b64", "ciphertext_b64", "tag_b64", mode="before")
    @classmethod
    def _validate_base64(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            base64.b64decode(v, validate=True)
        except Exception:
            raise ValueError("field must be valid base64")
        return v

    def model_post_init(self, __context) -> None:
        has_token = self.ciphertext_token is not None
        has_legacy = self.edek_id is not None
        if not has_token and not has_legacy:
            raise ValueError(
                "Provide either 'ciphertext_token' (recommended) "
                "or the legacy fields 'edek_id', 'iv_b64', 'ciphertext_b64', 'tag_b64'"
            )
        if has_legacy:
            missing = [f for f in ("iv_b64", "ciphertext_b64", "tag_b64")
                       if getattr(self, f) is None]
            if missing:
                raise ValueError(
                    f"Legacy decrypt is missing required fields: {missing}. "
                    "Use 'ciphertext_token' instead to avoid this."
                )


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
    ciphertext_token: str       # opaque token — store and echo back to /decrypt; never decode client-side
    created_at: str


class ConsumerRevealRequest(BaseModel):
    reveal_as: str              # which app_id is asking to decrypt — exercises the same grant model as /decrypt
    end_user_id: str | None = None   # logged-in user who triggered the reveal; passed through to audit log


class ConsumerRevealResponse(BaseModel):
    id: int
    account_number: str   # decrypted on demand, never written back to the table
