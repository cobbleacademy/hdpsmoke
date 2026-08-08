-- Adds the DEK Name column to the Consumer Application Table, mirroring
-- edek_records.dek_name (V7__add_dek_name_to_edek_records.sql). No behavior
-- change: DemoController.createConsumerAccount() still passes a null dekName
-- into /encrypt, so this column stays null for every row today -- it exists so
-- the Consumer Application Table's column set matches the EDEK Records table's,
-- and so a future change to name-reuse consumer accounts has somewhere to land.
ALTER TABLE consumer_customer_accounts ADD COLUMN dek_name VARCHAR(256);
