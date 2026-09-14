-- Closes a real gap in the data_classification field: it's free text today
-- (EncryptRequest.dataClassification's own comment: "drives audit/retention
-- queries, never enforced here"), and the only existing check
-- (EncryptionService.checkClassificationMatch / DekIssueService's
-- equivalent) only prevents RELABELING an already-named dek_name -- it never
-- runs on first mint. So any app can mint a brand-new dek_name and stamp any
-- classification value on it at all, with zero admission control -- a gap
-- confirmed while reviewing the V14 grant model, not a hypothetical.
--
-- app_classification_grants is an ALLOW-LIST, not a grant between two apps
-- the way app_grants/app_dek_grants are (there's no "owner" concept for a
-- classification label the way there is for a dek_name) -- a flat
-- (app_id, data_classification) row means "this app may declare this
-- classification"; no row means it hasn't been approved to.
--
-- PHASE 1 (this migration + the accompanying code): SHADOW MODE ONLY. Rows
-- inserted here are not yet enforced against anything -- ClassificationGovernanceService
-- only LOGS when an app mints under a classification with no approved row,
-- so real usage can be observed and backfilled before Phase 2 turns on
-- actual rejection. This mirrors kek_registry's own "unprovisioned
-- combinations fail closed, except the one deliberate transition exception"
-- precedent (V11's comment) -- rather than repeat that exception here,
-- shadow mode is the transition mechanism instead, since unlike kek_registry
-- (which has a legacy default to fall back to), there's no sane default
-- classification to fall back to for an unapproved one.
--
-- granted_by: who approved this row -- an admin identity, never the app
-- itself (enforced at the application layer, not by this schema -- see the
-- planned POST /admin/apps/classifications endpoint, gated by its own
-- authority, same reasoning as provision_app_keys being kept separate from
-- manage_apps).
--
-- Lives in access_schema (governance/authorization), not crypto_schema
-- (key material/EDEK storage) -- same schema app_grants/app_dek_grants use,
-- for the same reason: this is an access-control table, not a crypto one.
CREATE TABLE ${access_schema}.app_classification_grants (
    app_id              VARCHAR(128) NOT NULL,
    data_classification VARCHAR(32)  NOT NULL,
    granted_by          VARCHAR(128) NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (app_id, data_classification)
);
