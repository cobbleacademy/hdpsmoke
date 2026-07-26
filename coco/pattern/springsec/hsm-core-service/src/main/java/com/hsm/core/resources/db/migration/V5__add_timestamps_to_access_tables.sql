-- app_registrations and app_decrypt_grants were the only two tables in the
-- schema with no timestamp columns at all -- see java/docs/ADMIN_OPERATIONS.md.
-- Nullable, no backfill, same pattern as V2's fingerprint column: existing
-- rows predate this migration and simply have NULL timestamps going forward.
--
-- app_decrypt_grants gets created_at only, not updated_at -- grants are
-- add/remove, never mutated in place (AppRegistryService.addGrant/removeGrant
-- are insert/delete, not update).

ALTER TABLE ${access_schema}.app_registrations ADD COLUMN created_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE ${access_schema}.app_registrations ADD COLUMN updated_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE ${access_schema}.app_decrypt_grants ADD COLUMN created_at TIMESTAMP WITH TIME ZONE;
