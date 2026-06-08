package databricks.abac

import future.keywords.if
import future.keywords.in

has_tag_value(col, key, val) if { input.column_tags[col][key] == val }
has_tag(col, key) if { _ := input.column_tags[col][key] }

mask_pii_string(column_value) := "***REDACTED***" if { }

region_filter_abac(region) if { "analysts-east" in input.groups; region == "east" }
region_filter_abac(region) if { "analysts-west" in input.groups; region == "west" }

row_visible["mask_all_pii_strings"] if {
    input.catalog == "demos"
    not "pii-readers" in input.groups
    some c
    has_tag_value(c, "demo_sensitivity", "pii")
    mask_pii_string(input.row[c])
}

column_masked["mask_all_pii_strings"][c] := mask_pii_string(input.row[c]) if {
    input.catalog == "demos"
    not "pii-readers" in input.groups
    has_tag_value(c, "demo_sensitivity", "pii")
}

row_visible["region_row_filter"] if {
    input.catalog == "demos"
    not "pii-readers" in input.groups
    some region
    has_tag_value(region, "demo_row_scope", "region")
    region_filter_abac(input.row[region])
}

column_masked["region_row_filter"][region] := region_filter_abac(input.row[region]) if {
    input.catalog == "demos"
    not "pii-readers" in input.groups
    has_tag_value(region, "demo_row_scope", "region")
    some ctx
    has_tag(ctx, "demo_row_scope")
}