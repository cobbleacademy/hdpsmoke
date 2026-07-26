-- Ported from migrations/versions/0002_add_fingerprint_to_edek_records.py (Alembic revision 0002).
-- First 8 bytes of SHA-256(iv || tag) as a 16-char hex string. Nullable so
-- pre-existing rows (written before this column existed) still decrypt --
-- the fingerprint pre-flight check is skipped when fingerprint IS NULL.

ALTER TABLE ${crypto_schema}.edek_records ADD COLUMN fingerprint VARCHAR(16);
