package databricks.abac

import future.keywords.if
import future.keywords.in

has_tag_value(col, key, val) if { input.column_tags[col][key] == val }

column_masked["test"][c] := "REDACTED" if {
    input.catalog == "demos"
    has_tag_value(c, "pii", "ssn")
}