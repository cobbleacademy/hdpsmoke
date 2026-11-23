"""
The Delta source (java/docs/DEIDENTIFICATION.md section 17.3): a table or a SQL query,
with only the profile's columns selected and every value turned into the string the
Java job's JDBC source would have produced (section 17.2, "values as strings").

Spark casts most types to exactly Java's text, because Spark runs on the JVM:
integers, booleans, dates (``yyyy-MM-dd``), doubles and floats (``Double.toString``,
``Float.toString``). Two need a fix-up in Python after the cast, done by
``to_java_string``:

- decimals: Java's ``BigDecimal.toPlainString`` (never ``1E-7``);
- timestamps: Java's ``LocalDateTime.toString`` (``2026-11-02T10:15``: a ``T``, seconds
  left out when zero, the fraction in groups of 3 digits). Timestamps with a time
  zone are read in the session time zone, which the job sets to UTC.

Binary and complex columns (arrays, maps, structs) are refused, naming the column.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

from .errors import DeidError

# Spark type name (DataType.typeName / simpleString start) -> fix-up kind
_PLAIN = {"string", "varchar", "char", "integer", "int", "long", "bigint", "short", "smallint", "byte", "tinyint",
          "boolean", "date", "double", "float"}
_FIXUP = {"decimal": "decimal", "timestamp": "timestamp", "timestamp_ntz": "timestamp"}
_TIMESTAMP = re.compile(r"(-?\d{4,})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?")


def kind_of(type_name: str) -> str:
    """'plain', 'decimal' or 'timestamp' for a Spark type's simpleString; raises for types a CSV can't hold."""
    base = type_name.split("(")[0].strip().lower()
    if base in _PLAIN:
        return "plain"
    if base in _FIXUP:
        return _FIXUP[base]
    raise ValueError(base)


def check_columns(schema_fields: list[tuple[str, str]], wanted: list[str]) -> dict:
    """
    ``schema_fields``: (name, Spark simpleString) of the source. Returns the fix-up
    kind per wanted column, matched case-insensitively as the Java JDBC source does.
    Raises DeidError naming a binary or complex column.
    """
    by_lower = {}
    for name, type_name in schema_fields:
        by_lower.setdefault(name.lower(), (name, type_name))
    kinds = {}
    for w in wanted:
        hit = by_lower.get(w.lower())
        if hit is None:
            continue  # the validator reports it
        name, type_name = hit
        try:
            kinds[w] = (name, kind_of(type_name))
        except ValueError:
            base = type_name.split("(")[0].split("<")[0].strip().lower()
            what = "binary" if base == "binary" else "a " + base
            raise DeidError.bad_value("column '" + name + "' is " + what + "; it can't be written to a CSV "
                                      "(convert it in --source-query)") from None
    return kinds


def to_java_string(value: str | None, kind: str) -> str | None:
    """Fixes up Spark's cast-to-string text of a decimal or timestamp to Java's form."""
    if value is None or kind == "plain":
        return value
    if kind == "decimal":
        try:
            return format(Decimal(value), "f")
        except InvalidOperation:
            return value
    m = _TIMESTAMP.fullmatch(value)
    if not m:
        return value
    y, mo, d, h, mi, s, frac = m.groups()
    out = y + "-" + mo + "-" + d + "T" + h + ":" + mi
    nanos = int((frac or "").ljust(9, "0")) if frac else 0
    if (s and int(s)) or nanos:
        out += ":" + (s or "00")
    if nanos:
        if nanos % 1_000_000 == 0:
            out += "." + str(nanos // 1_000_000).rjust(3, "0")
        elif nanos % 1000 == 0:
            out += "." + str(nanos // 1000).rjust(6, "0")
        else:
            out += "." + str(nanos).rjust(9, "0")
    return out


def select_exprs(kinds: dict) -> list[str]:
    """SQL expressions selecting each wanted column, cast to string, under its profile name."""
    return ["CAST(`" + name.replace("`", "``") + "` AS STRING) AS `" + wanted.replace("`", "``") + "`"
            for wanted, (name, _) in kinds.items()]


def read(spark, *, table: str | None, query: str | None, wanted: list[str]):
    """
    The source DataFrame with the wanted columns as strings, the fix-up kinds, and the
    source's full column list (for the validator and the manifest).
    """
    if bool(table) == bool(query):
        raise ValueError("give exactly one of --source-table or --source-query")
    df = spark.table(table) if table else spark.sql(query)
    schema = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
    all_columns = [n for n, _ in schema]
    kinds = check_columns(schema, wanted)
    selected = df.selectExpr(*select_exprs(kinds)) if kinds else df.limit(0)
    return selected, {w: k for w, (_, k) in kinds.items()}, all_columns
