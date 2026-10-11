"""
One-time provisioning script: generate a 32-byte Cache Encryption Key (CEK)
and store it in Azure Key Vault as a secret.

Run this ONCE per environment (dev / staging / prod) before deploying the
Redis DEK cache feature. After this runs, the HSM service reads the secret
at startup via the same DefaultAzureCredential — no further manual steps.

Prerequisites:
  pip install azure-identity azure-keyvault-secrets

Usage:
  python scripts/provision_dek_cache_key.py \
      --vault-url https://<your-vault>.vault.azure.net/ \
      --secret-name hsm-dek-cache-key

The caller must have the "Key Vault Secrets Officer" role (or equivalent)
on the target vault. The Service SPN only needs "Key Vault Secrets User"
(secrets/get) — it cannot write or list secrets.
"""

import argparse
import base64
import os
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description="Provision CEK secret in Azure Key Vault")
    parser.add_argument("--vault-url", required=True,
                        help="Regular Azure Key Vault URL: https://<name>.vault.azure.net/")
    parser.add_argument("--secret-name", default="hsm-dek-cache-key",
                        help="Secret name in Key Vault (default: hsm-dek-cache-key)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Generate and print the key without writing to Key Vault")
    args = parser.parse_args()

    if "managedhsm.azure.net" in args.vault_url:
        print("ERROR: --vault-url must be a regular Key Vault (*.vault.azure.net), "
              "not a Managed HSM. Managed HSM does not support the Secrets API.")
        sys.exit(1)

    # Generate 32 cryptographically random bytes (256-bit key for AES-256-GCM)
    cek_bytes = os.urandom(32)
    cek_b64 = base64.b64encode(cek_bytes).decode()

    print(f"Generated CEK: {len(cek_bytes)} bytes (base64 length: {len(cek_b64)})")

    if args.dry_run:
        print(f"DRY RUN — key NOT written to Key Vault.")
        print(f"CEK (base64): {cek_b64}")
        return

    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
    except ImportError:
        print("ERROR: Install dependencies first:  pip install azure-identity azure-keyvault-secrets")
        sys.exit(1)

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=args.vault_url, credential=credential)

    # Check if secret already exists — never overwrite silently
    try:
        existing = client.get_secret(args.secret_name)
        print(f"ERROR: Secret '{args.secret_name}' already exists in {args.vault_url}.")
        print(f"  Version: {existing.properties.version}")
        print(f"  Created: {existing.properties.created_on}")
        print("If you intend to rotate the CEK, delete and purge the existing secret first, "
              "then flush the Redis cache namespace before re-running this script.")
        sys.exit(1)
    except Exception as exc:
        # ResourceNotFoundError is expected on first run — anything else is a real error
        if "ResourceNotFound" not in type(exc).__name__:
            raise

    secret = client.set_secret(
        name=args.secret_name,
        value=cek_b64,
        content_type="application/octet-stream; encoding=base64",
        tags={
            "purpose": "dek-cache-encryption-key",
            "key-length-bits": "256",
            "managed-by": "hsm-encryption-service",
        },
    )

    print(f"CEK stored successfully.")
    print(f"  Vault   : {args.vault_url}")
    print(f"  Secret  : {secret.name}")
    print(f"  Version : {secret.properties.version}")
    print(f"  Created : {secret.properties.created_on}")
    print()
    print("Next steps:")
    print(f"  1. Grant the Service SPN 'Key Vault Secrets User' on this vault")
    print(f"     (secrets/get on secret '{args.secret_name}' only — not list/set/delete)")
    print(f"  2. Add  azure_keyvault_secret_url: {args.vault_url}  to config / values.yaml")
    print(f"  3. Set  dek_cache_key_secret_name: {args.secret_name}  in config")
    print(f"  4. Enable  dek_cache_enabled: true  after Redis is wired up")


if __name__ == "__main__":
    main()
