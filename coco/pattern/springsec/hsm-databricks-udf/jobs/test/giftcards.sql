-- Test schema, volumes and source table for a first hsm-deid run on a workspace
-- (DEID_DEPLOYMENT.md section 11). Replace <catalog>. Made-up values only.

CREATE SCHEMA IF NOT EXISTS <catalog>.deid_test;
CREATE VOLUME IF NOT EXISTS <catalog>.deid_test.libs;      -- the wheel
CREATE VOLUME IF NOT EXISTS <catalog>.deid_test.deid;      -- profiles/
CREATE VOLUME IF NOT EXISTS <catalog>.deid_test.deid_out;  -- one folder per profile

CREATE OR REPLACE TABLE <catalog>.deid_test.giftcards (
  id INT, member_id STRING, first_name STRING, dob DATE, zip STRING,
  amount DECIMAL(10,2), status STRING, issued_date TIMESTAMP);

INSERT INTO <catalog>.deid_test.giftcards VALUES
  (1, 'M1234567', 'Jane', DATE'1985-03-14', '60614-2231',  25.00, 'ACTIVE',   TIMESTAMP'2026-11-02 10:15:00'),
  (2, 'M7654321', 'Raj',  DATE'1936-07-01', '03601',      100.00, 'REDEEMED', TIMESTAMP'2026-10-18 08:00:00'),
  (3, 'M1234567', 'Jane', DATE'1985-03-14', '60614',       -5.00, 'ADJUSTED', TIMESTAMP'2026-11-05 12:00:00.250'),
  (4, 'M5550001', NULL,   NULL,             NULL,          10.00, 'ACTIVE',   NULL);
