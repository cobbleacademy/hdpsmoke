-- Tier 3 bulk PoC (java/docs/BULK_OPERATIONS.md): hsm-bulk-service transport-wraps
-- each issued/unwrapped DEK with the calling app's own public key so the raw key
-- never crosses the wire in the clear. Nullable, no backfill -- only apps
-- provisioned for dek_issue/dek_unwrap need a key; every existing row simply has
-- NULL here and is unaffected (same pattern as V2's fingerprint column).

ALTER TABLE ${access_schema}.app_registrations ADD COLUMN public_key_pem TEXT;
