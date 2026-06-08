package databricks.abac

import future.keywords.if
import future.keywords.in

# Input schema:
# input.catalog:     string               — catalog name (e.g. "demos")
# input.schema:      string               — "catalog.schema"
# input.principal:   string               — user email or service principal
# input.groups:      [string]             — account-level group memberships
# input.column_tags: {col: {key: value}}  — live tag metadata from Unity Catalog
# input.row:         {col: value}         — row under evaluation

# ── Tag helper rules ──────────────────────────────────────────────────────────
has_tag_value(col, key, val) if { input.column_tags[col][key] == val }
has_tag(col, key) if { _ := input.column_tags[col][key] }

# ── UDF: mask_pii_string ──────────────────────────────────────────────────────
# Source: CREATE FUNCTION mask_pii_string(column_value STRING) RETURNS STRING
mask_pii_string(_) := "***REDACTED***"

# ── UDF: region_filter_abac ───────────────────────────────────────────────────
# Source: CREATE FUNCTION region_filter_abac(region STRING) RETURNS BOOLEAN
region_filter_abac(region) if {
    "analysts-east" in input.groups
    region == "east"
}

region_filter_abac(region) if {
    "analysts-west" in input.groups
    region == "west"
}

# ── Policy: mask_all_pii_strings (COLUMN MASK) ────────────────────────────────
# ON CATALOG demos | TO `account users` EXCEPT `pii-readers`
# MATCH COLUMNS has_tag_value('demo_sensitivity','pii') AS c | ON COLUMN c
column_masked["mask_all_pii_strings"][c] := mask_pii_string(input.row[c]) if {
    input.catalog == "demos"
    not "pii-readers" in input.groups
    has_tag_value(c, "demo_sensitivity", "pii")
}

# ── Policy: region_row_filter (ROW FILTER) ────────────────────────────────────
# ON CATALOG demos | TO `account users` EXCEPT `pii-readers`
# MATCH COLUMNS has_tag_value('demo_row_scope','region') AS region | USING COLUMNS (region)
row_visible["region_row_filter"] if {
    input.catalog == "demos"
    not "pii-readers" in input.groups
    some region_col
    has_tag_value(region_col, "demo_row_scope", "region")
    region_filter_abac(input.row[region_col])
}