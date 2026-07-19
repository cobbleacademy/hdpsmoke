"""
Azure Key Vault HSM client for KEK wrap/unwrap operations.

The KEK never leaves the HSM boundary. Only the wrapped DEK (EDEK)
is returned to the service. Authentication uses Managed Identity —
no static credentials.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os

from azure.identity.aio import DefaultAzureCredential, ManagedIdentityCredential, WorkloadIdentityCredential
from azure.keyvault.keys.aio import KeyClient
from azure.keyvault.keys.crypto.aio import CryptographyClient
from azure.keyvault.keys.crypto import KeyWrapAlgorithm
from azure.keyvault.secrets.aio import SecretClient

from app.config import Settings


# RSA-OAEP-SHA-256 is the FIPS-approved wrapping algorithm for RSA keys in AKV HSM
_WRAP_ALGORITHM = KeyWrapAlgorithm.rsa_oaep_256

_TOKEN_FILE = "/var/run/secrets/azure/tokens/azure-identity-token"


def _decode_token_claims(token_file: str) -> dict:
    """Decode the injected JWT payload without verifying signature."""
    try:
        payload = open(token_file).read().split(".")[1]
        payload += "=" * (-len(payload) % 4)  # fix padding
        return json.loads(base64.b64decode(payload))
    except Exception:
        return {}


def _build_credential(settings: Settings):
    """
    Build the Azure credential without requiring env var injection.

    Resolution order for each parameter:
      client_id  : config → AZURE_CLIENT_ID env → JWT appid/azp claim
      tenant_id  : config → AZURE_TENANT_ID env → JWT tid claim
      token_file : AZURE_FEDERATED_TOKEN_FILE env → well-known path

    If all three resolve, uses WorkloadIdentityCredential directly.
    Falls back to DefaultAzureCredential only when the token file is absent
    (local dev / demo mode).
    """
    token_file = os.environ.get("AZURE_FEDERATED_TOKEN_FILE", _TOKEN_FILE)

    claims = _decode_token_claims(token_file) if os.path.exists(token_file) else {}

    client_id = (
        settings.azure_client_id
        or os.environ.get("AZURE_CLIENT_ID")
        or claims.get("appid")
        or claims.get("azp")
    )
    tenant_id = (
        settings.azure_tenant_id
        or os.environ.get("AZURE_TENANT_ID")
        or claims.get("tid")
    )

    if client_id and tenant_id and os.path.exists(token_file):
        return WorkloadIdentityCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            token_file_path=token_file,
        )

    # No federated credential configured — fall back to node managed identity
    # via IMDS (169.254.169.254). Requires the AKS node pool to have a
    # user-assigned managed identity with Key Vault permissions.
    if settings.azure_client_id:
        return ManagedIdentityCredential(client_id=settings.azure_client_id)

    return DefaultAzureCredential()


class KEKClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._credential = _build_credential(settings)
        self._key_client = KeyClient(
            vault_url=settings.azure_keyvault_url,
            credential=self._credential,
        )
        # Secrets API requires a regular Key Vault (*.vault.azure.net).
        # Managed HSM does not support secrets — use the dedicated secret vault
        # URL when provided, otherwise fall back to the same vault URL (valid
        # only if azure_keyvault_url is a regular vault, not an MHSM endpoint).
        _secret_vault_url = settings.azure_keyvault_secret_url or settings.azure_keyvault_url
        self._secret_client = SecretClient(
            vault_url=_secret_vault_url,
            credential=self._credential,
        )
        self._crypto_client: CryptographyClient | None = None
        self._lock = asyncio.Lock()

    async def _get_crypto_client(self) -> CryptographyClient:
        async with self._lock:
            if self._crypto_client is None:
                version = self._settings.azure_kek_version or None
                key = await self._key_client.get_key(
                    self._settings.azure_kek_name,
                    version=version,
                )
                self._crypto_client = CryptographyClient(
                    key=key,
                    credential=self._credential,
                )
            return self._crypto_client

    async def wrap_dek(self, dek: bytes) -> tuple[bytes, str]:
        """
        Wrap a DEK using the HSM-bound KEK.
        Returns (edek_bytes, kek_version).
        """
        client = await self._get_crypto_client()
        result = await client.wrap_key(_WRAP_ALGORITHM, dek)
        return result.encrypted_key, result.key_id.split("/")[-1]

    async def unwrap_dek(self, edek: bytes, kek_version: str) -> bytes:
        """
        Unwrap an EDEK using the specific KEK version it was wrapped with.
        Old versions remain usable after rotation until all EDEKs are re-wrapped.
        """
        key_id = (
            f"{self._settings.azure_keyvault_url.rstrip('/')}"
            f"/keys/{self._settings.azure_kek_name}/{kek_version}"
        )
        versioned_client = CryptographyClient(
            key=key_id,
            credential=self._credential,
        )
        result = await versioned_client.unwrap_key(_WRAP_ALGORITHM, edek)
        return result.key

    async def get_current_kek_version(self) -> str:
        key = await self._key_client.get_key(self._settings.azure_kek_name)
        return key.properties.version or ""

    async def fetch_secret(self, secret_name: str) -> str:
        """Retrieve a secret value from Azure KV Secrets (vault.azure.net)."""
        secret = await self._secret_client.get_secret(secret_name)
        return secret.value or ""

    async def fetch_secret_with_version(self, secret_name: str) -> tuple[str, str]:
        """Return (value, kv_version) for a secret.

        kv_version is the last path segment of the secret id, e.g.:
          https://vault.azure.net/secrets/cek-alpha/3f8a2b... → "3f8a2b..."
        Used to construct Redis keys as {slot}:{kv_version}:{edek_id}.
        """
        secret = await self._secret_client.get_secret(secret_name)
        value = secret.value or ""
        kv_version = (secret.properties.id or "").rstrip("/").rsplit("/", 1)[-1]
        return value, kv_version

    async def close(self) -> None:
        await self._key_client.close()
        await self._secret_client.close()
        await self._credential.close()
        if self._crypto_client:
            await self._crypto_client.close()
