"""
The Java/Python cross-check (java/docs/DEIDENTIFICATION.md section 17.2): this job, on
the input table and profiles in java/hsm-crypto-client/src/test/resources/deid-crosscheck,
must write the same CSVs the Java job writes there (DeidCrossCheckTest checks the Java
side against the same expected files).

Every surrogate comes from surrogates.json, loaded first, so the files match byte for
byte. Spark doesn't keep row order, so rows are compared as a sorted list; the header,
each row's bytes and the line endings must be identical. Skipped without Spark and
Postgres (tests/deid/spark/run-tests.sh).
"""

import json

import pytest

from hsm_databricks_udf.deid.job import DeidJob, Settings
from hsm_databricks_udf.deid.surrogate_store import connect

from conftest import FakeHsm, REPO, SURROGATE_KEY_ID, clock_2026

DIR = REPO / "java" / "hsm-crypto-client" / "src" / "test" / "resources" / "deid-crosscheck"
INPUT = json.loads((DIR / "input.json").read_text(encoding="utf-8"))
SPARK_TYPES = {"integer": "int", "date": "date", "double": "double", "real": "float", "bigint": "bigint",
               "boolean": "boolean", "timestamp": "timestamp"}


def spark_type(sql_type: str) -> str:
    if sql_type.startswith("varchar"):
        return "string"
    if sql_type.startswith("decimal"):
        return sql_type
    return SPARK_TYPES[sql_type]


def records(text: str, ending: str) -> list[str]:
    """Splits a CSV file into records at line endings outside quotes (keeps each record's bytes)."""
    out, cur, quoted, i = [], [], False, 0
    while i < len(text):
        if text[i] == '"':
            quoted = not quoted
        if not quoted and text.startswith(ending, i):
            out.append("".join(cur))
            cur = []
            i += len(ending)
            continue
        cur.append(text[i])
        i += 1
    assert not cur, "the file ends with a line ending"
    return out


def comparable(m: dict) -> dict:
    """As DeidCrossCheckTest.comparable; the file hash depends on row order, so it is left out here."""
    out = {k: m[k] for k in ("profile", "keys", "fields", "dropped_by_default") if k in m}
    for k in ("surrogates", "decrypt"):
        if k in m:
            out[k] = m[k]
    out["output"] = {k: v for k, v in m["output"].items() if k != "sha256"}
    return out


@pytest.fixture
def crosscheck_table(spark):
    from pyspark.sql.types import StringType, StructField, StructType
    cols = INPUT["columns"]
    raw = spark.createDataFrame([tuple(r) for r in INPUT["rows"]],
                                StructType([StructField(c["name"], StringType()) for c in cols]))
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    raw.selectExpr(*["CAST(`" + c["name"] + "` AS " + spark_type(c["type"]) + ") AS `" + c["name"] + "`"
                     for c in cols]).createOrReplaceTempView("giftcards")


@pytest.fixture
def seeded(pg):
    seeds = json.loads((DIR / "surrogates.json").read_text(encoding="utf-8"))
    conn = connect(pg)
    with conn.cursor() as cur:
        for r in seeds["rows"]:
            cur.execute("INSERT INTO " + pg.qualified_table + " (scope, domain, lookup_hash, value_enc, key_edek_id, "
                        "surrogate, created_run_id) VALUES (%s, %s, %s, %s, CAST(%s AS uuid), %s, 'seed')",
                        (r["scope"], r["domain"], bytes.fromhex(r["lookup_hash"]), bytes.fromhex(r["value_enc"]),
                         seeds["key_edek_id"], r["surrogate"]))
    conn.close()
    return pg


@pytest.mark.parametrize("name", ["zen", "acme", "types"])
def test_the_python_job_writes_what_the_java_job_writes(spark, crosscheck_table, seeded, tmp_path, name):
    r = DeidJob(spark, Settings(profile_path=str(DIR / "profiles" / (name + ".yml")), output_root=str(tmp_path),
                                source_table="giftcards", key_column="id", surrogate_key_edek_id=str(SURROGATE_KEY_ID),
                                surrogate_db=seeded, surrogate_partitions=3, triggered_by="crosscheck"),
                hsm=FakeHsm(), clock=clock_2026).run()
    expected = (DIR / "expected" / (name + ".csv")).read_bytes().decode("utf-8")
    actual = r.csv.read_bytes().decode("utf-8")
    ending = "\r\n" if expected.split("\n")[0].endswith("\r") else "\n"
    exp, act = records(expected, ending), records(actual, ending)
    assert act[0] == exp[0], "header"
    assert sorted(act[1:]) == sorted(exp[1:])
    assert r.csv.name == name + "_20261120.csv"
    java_manifest = json.loads((DIR / "expected" / (name + ".manifest.json")).read_text(encoding="utf-8"))
    java_manifest["output"].pop("sha256")
    assert comparable(r.manifest) == java_manifest
