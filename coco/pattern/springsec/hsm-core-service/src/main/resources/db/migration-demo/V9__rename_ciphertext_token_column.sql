-- Requirement 6 follow-up: the earlier round deliberately kept ciphertext_token
-- as-is (additive envelope only, no rename -- see V8's sibling round in
-- dev-status-seed.json) because it's a breaking wire-contract change. Explicit
-- follow-up decision: apply the rename across the board anyway, consumer_accounts
-- column included, since a real deployment would need the same ALTER either way
-- and there's no live external caller of this demo-only table to break.
ALTER TABLE consumer_customer_accounts RENAME COLUMN ciphertext_token TO ciphertext;
