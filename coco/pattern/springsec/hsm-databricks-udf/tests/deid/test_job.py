"""
The Databricks DEID job end to end on local Spark and Postgres (skipped without them:
tests/deid/spark/run-tests.sh): the gift-card table of the Java DeidJobTest, two of
its columns encrypted, exported for a reporting vendor (Safe Harbor, surrogates) and
a fulfillment vendor (tokens and masks).
"""

import csv
import datetime as dt
import io
import json
import logging
from decimal import Decimal

import pytest

from hsm_databricks_udf.deid import keys
from hsm_databricks_udf.deid.errors import DeidError, DeidValidationError
from hsm_databricks_udf.deid.job import DeidJob, Settings
from hsm_databricks_udf.deid.methods import FieldOptions, TokenizeTransform

from conftest import COLUMN_DEK_ID, FakeHsm, SURROGATE_KEY_ID, TOKEN_KEY_ID, clock_2026, dek, pg_count

ZEN = """
id: zen-reporting
version: 1
vendor: zen-analytics
hipaa-basis: safe-harbor
surrogate-scope: zen-analytics
fields:
  - { name: member_id,   phi: member-id,   method: surrogate, domain: member }
  - { name: card_number, phi: card-number, decrypt: true, method: surrogate, domain: card, normalize: digits }
  - { name: dob,         phi: birth-date,  method: generalize, to: birth-year }
  - { name: issued_date, phi: date,        method: generalize, to: year }
  - { name: zip,         phi: zip,         method: generalize, to: zip3 }
  - { name: email,       phi: email,       method: drop }
  - { name: amount,                        method: passthrough }
  - { name: status,                        method: passthrough }
output:
  file: "giftcards_zen_{yyyyMMdd}.csv"
  rename: { member_id: member_ref }
"""

ACME = """
id: acme-fulfillment
version: 1
vendor: acme-rewards
hipaa-basis: minimum-necessary
token-key-edek-id: 6f1c0000-0000-4000-8000-000000000001
token-domain: member
fields:
  - { name: member_id,   phi: member-id,   method: tokenize }
  - { name: first_name,  phi: name,        method: passthrough }
  - { name: email,       phi: email,       decrypt: true, method: mask, keep: domain, keep-first: 1 }
  - { name: card_number, phi: card-number, decrypt: true, method: mask, keep-last: 4 }
  - { name: amount,                        method: passthrough }
output: { file: "giftcards_acme_{yyyyMMdd}.csv", formula-guard: false }
"""

PLAIN = """
id: plain
version: 1
hipaa-basis: minimum-necessary
fields:
  - { name: first_name,  phi: name, method: mask, keep-first: 1 }
  - { name: zip,         phi: zip,  method: generalize, to: zip3 }
  - { name: amount,                 method: passthrough }
  - { name: issued_date,            method: passthrough }
output: { file: "plain_{run}.csv" }
"""

SENSITIVE = ["M1234567", "M7654321", "Jane", "jane.doe", "raj.p", "6035712345678901", "6035719876543210", "4821",
             "1985-03-14", "60614"]

ROWS = [
    (1, "M1234567", "Jane", "jane.doe@mail.com", "1985-03-14", "60614-2231", "6035712345678901", "4821", "25.00",
     "ACTIVE", "2026-11-02 10:15:00"),
    (2, "M7654321", "Raj", "raj.p@corp.org", "1936-07-01", "03601", "6035719876543210", "1122", "100.00",
     "REDEEMED", "2026-10-18 08:00:00"),
    (3, "M1234567", "Jane", "jane.doe@mail.com", "1985-03-14", "60614", "6035712345678901", "4821", "-5.00",
     "ADJUSTED", "2026-11-05 12:00:00.250"),
    (4, "M5550001", None, None, None, None, None, None, "10.00", "ACTIVE", None),
]


@pytest.fixture
def table(spark):
    """The gift-card table, email and card_number encrypted, as a temp view ``giftcards``."""
    from pyspark.sql.types import DateType, DecimalType, IntegerType, StringType, StructField, StructType, TimestampType
    hsm = FakeHsm()
    schema = StructType([StructField("id", IntegerType()), StructField("member_id", StringType()),
                         StructField("first_name", StringType()), StructField("email", StringType()),
                         StructField("dob", DateType()), StructField("zip", StringType()),
                         StructField("card_number", StringType()), StructField("card_pin", StringType()),
                         StructField("amount", DecimalType(10, 2)), StructField("status", StringType()),
                         StructField("issued_date", TimestampType())])
    rows = []
    for r in ROWS:
        rows.append((r[0], r[1], r[2], hsm.encrypt(r[3]) if r[3] else None,
                     dt.date.fromisoformat(r[4]) if r[4] else None, r[5], hsm.encrypt(r[6]) if r[6] else None,
                     r[7], Decimal(r[8]), r[9], dt.datetime.fromisoformat(r[10]) if r[10] else None))
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.createDataFrame(rows, schema).createOrReplaceTempView("giftcards")
    return hsm


def job(spark, tmp_path, yaml, *, pg=None, hsm=None, dry_run=False, name="p", key_column="id", out="out", **kw):
    profile = tmp_path / (name + ".yml")
    profile.write_text(yaml, encoding="utf-8")
    settings = Settings(profile_path=str(profile), output_root=str(tmp_path / out), source_table="giftcards",
                        key_column=key_column, surrogate_key_edek_id=str(SURROGATE_KEY_ID), surrogate_db=pg,
                        surrogate_partitions=3, dry_run=dry_run, triggered_by="pipeline-42", **kw)
    return DeidJob(spark, settings, hsm=hsm, clock=clock_2026)


def read_csv(path):
    text = path.read_text(encoding="utf-8")
    rows = list(csv.reader(io.StringIO(text)))
    return text, rows[0], rows[1:]


def test_safe_harbor_export_from_a_table(spark, table, tmp_path, pg, caplog):
    caplog.set_level(logging.INFO)
    r = job(spark, tmp_path, ZEN, pg=pg, hsm=table).run()
    text, header, rows = read_csv(r.csv)
    assert r.csv.name == "giftcards_zen_20261120.csv"
    assert header == ["member_ref", "card_number", "dob", "issued_date", "zip", "amount", "status"]
    by_amount = {row[5]: row for row in rows}
    assert set(by_amount) == {"25.00", "100.00", "'-5.00", "10.00"}
    jane1, raj, jane2, m3 = by_amount["25.00"], by_amount["100.00"], by_amount["'-5.00"], by_amount["10.00"]
    assert jane1[0] == jane2[0] and jane1[1] == jane2[1], "the same member and card get the same surrogates"
    assert len({jane1[0], raj[0], m3[0]}) == 3
    assert jane1[0].startswith("S-") and len(jane1[0]) == 13
    assert jane1[2:5] == ["1985", "2026", "606"]
    assert raj[2:5] == ["<=1936", "2026", "000"]
    assert m3[1:5] == ["", "", "", ""]
    assert pg_count(pg) == 5, "three members and two cards"
    for s in SENSITIVE:
        assert s not in text

    m = r.manifest
    assert json.loads(r.manifest_file.read_text())["run_id"] == m["run_id"]
    assert m["output"] == {"file": "giftcards_zen_20261120.csv", "rows_in": 4, "rows_out": 4, "rows_skipped": 0,
                           "sha256": m["output"]["sha256"]}
    assert m["surrogates"] == {"new": 5, "existing": 0}
    assert m["decrypt"] == {"fields": ["card_number"], "distinct_edek_ids": 1}
    assert m["dropped_by_default"] == ["id", "first_name", "card_pin"]
    dob = next(f for f in m["fields"] if f["source"] == "dob")
    assert (dob["values"], dob["empty"], dob["capped"], dob["cutoff_year"]) == (4, 1, 1, 1936)
    assert next(f for f in m["fields"] if f["source"] == "zip")["restricted"] == 1
    assert list((tmp_path / "out").iterdir()) and not (tmp_path / "out" / "_deid_tmp").exists()
    events = [json.loads(rec.getMessage()) for rec in caplog.records if rec.name.endswith(".audit")]
    assert events[-1]["event"] == "deid_export_completed" and events[-1]["rows"] == 4
    for rec in caplog.records:
        for s in SENSITIVE:
            assert s not in rec.getMessage()


def test_the_next_run_reuses_the_surrogates(spark, table, tmp_path, pg):
    first = read_csv(job(spark, tmp_path, ZEN, pg=pg, hsm=table, out="a").run().csv)[2]
    second = job(spark, tmp_path, ZEN, pg=pg, hsm=table, out="b").run()
    assert sorted(first) == sorted(read_csv(second.csv)[2])
    assert second.manifest["surrogates"] == {"new": 0, "existing": 5}
    assert pg_count(pg) == 5


def test_minimum_necessary_export_with_tokens_and_masks(spark, table, tmp_path):
    r = job(spark, tmp_path, ACME, hsm=table).run()
    _, header, rows = read_csv(r.csv)
    assert header == ["member_id", "first_name", "email", "card_number", "amount"]
    key = keys.hkdf_sha256(dek(1), None, keys.TOKENIZE_LABEL.encode(), 32)
    tok = TokenizeTransform.create(FieldOptions({}), key, "member")
    by_amount = {row[4]: row for row in rows}
    assert by_amount["25.00"] == [tok.apply("M1234567"), "Jane", "j*******@mail.com", "************8901", "25.00"]
    assert by_amount["-5.00"][0] == by_amount["25.00"][0], "formula guard off: -5.00 as is"
    assert by_amount["100.00"][1:4] == ["Raj", "r****@corp.org", "************3210"]
    assert by_amount["10.00"] == [tok.apply("M5550001"), "", "", "", "10.00"]
    assert r.manifest["keys"]["token"] == {"edek_id": str(TOKEN_KEY_ID), "epoch": 1}


def test_a_plain_profile_needs_no_hsm_core_and_writes_java_timestamps(spark, table, tmp_path):
    r = job(spark, tmp_path, PLAIN, hsm=None).run()
    _, header, rows = read_csv(r.csv)
    assert r.csv.name.startswith("plain_deid-20261120-1530-")
    assert sorted(rows) == sorted([["J***", "606", "25.00", "2026-11-02T10:15"],
                                   ["R**", "000", "100.00", "2026-10-18T08:00"],
                                   ["J***", "606", "'-5.00", "2026-11-05T12:00:00.250"],
                                   ["", "", "10.00", ""]])


def test_a_dry_run_writes_nothing_and_flags_unmarked_ciphertext(spark, table, tmp_path, pg, capsys):
    unmarked = ZEN.replace("decrypt: true, method: surrogate, domain: card, normalize: digits",
                           "method: surrogate, domain: card")
    r = job(spark, tmp_path, unmarked, pg=pg, hsm=table, dry_run=True, sample=2).run()
    printed = json.loads(capsys.readouterr().out)
    assert printed["review"]["ciphertext_not_marked_decrypt"] == ["card_number"]
    assert printed["review"]["sample_rows"] == 2
    assert printed["manifest"]["dry_run"] is True
    assert not (tmp_path / "out").exists() or list((tmp_path / "out").iterdir()) == []
    assert pg_count(pg) == 0
    assert r.review["passthrough"] == ["amount", "status"]


def test_a_real_run_refuses_unmarked_ciphertext(spark, table, tmp_path, pg):
    unmarked = ZEN.replace("decrypt: true, method: surrogate, domain: card, normalize: digits",
                           "method: surrogate, domain: card")
    with pytest.raises(DeidValidationError, match="holds HSM ciphertext but isn't decrypt: true"):
        job(spark, tmp_path, unmarked, pg=pg, hsm=table).run()
    assert pg_count(pg) == 0


def test_a_bad_value_on_a_worker_leaves_nothing_and_names_field_and_key(spark, table, tmp_path, pg, caplog):
    bad = ZEN.replace("to: year }", "to: year, input-format: 'dd/MM/yyyy' }")
    with pytest.raises(Exception) as e:
        job(spark, tmp_path, bad, pg=pg, hsm=table).run()
    assert "field 'issued_date', id " in str(e.value) and "not a date in the input-format" in str(e.value)
    assert not (tmp_path / "out" / "giftcards_zen_20261120.csv").exists()
    assert not (tmp_path / "out" / "_deid_tmp").exists()
    failed = [json.loads(r.getMessage()) for r in caplog.records if r.name.endswith(".audit")][-1]
    assert failed["event"] == "deid_export_failed"
    assert failed["reason"].startswith("field 'issued_date', id ")
    for s in SENSITIVE:
        assert s not in failed["reason"]


def test_a_missing_grant_fails_before_anything_is_written(spark, table, tmp_path, pg):
    no_grant = FakeHsm({k: v for k, v in table.deks.items() if k != COLUMN_DEK_ID})
    with pytest.raises(DeidError, match="no decrypt grant"):
        job(spark, tmp_path, ZEN, pg=pg, hsm=no_grant).run()
    assert pg_count(pg) == 0
    assert not (tmp_path / "out").exists() or list((tmp_path / "out").iterdir()) == []


def test_profile_problems_fail_before_reading(spark, table, tmp_path):
    with pytest.raises(DeidValidationError) as e:
        job(spark, tmp_path, ZEN.replace("method: drop", "method: passthrough").replace("name: status", "name: nope"),
            hsm=table).run()
    assert "field 'email' (phi: email): safe-harbor allows only drop" in e.value.problems
    assert "field 'nope' is not in the source" in e.value.problems


def test_an_existing_file_is_never_overwritten(spark, table, tmp_path):
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "giftcards_acme_20261120.csv").write_text("earlier run")
    with pytest.raises(RuntimeError, match="already exists"):
        job(spark, tmp_path, ACME, hsm=table).run()
    assert (tmp_path / "out" / "giftcards_acme_20261120.csv").read_text() == "earlier run"


def test_a_query_source(spark, table, tmp_path):
    profile = tmp_path / "q.yml"
    profile.write_text(PLAIN, encoding="utf-8")
    r = DeidJob(spark, Settings(profile_path=str(profile), output_root=str(tmp_path / "out"),
                                source_query="SELECT first_name, zip, amount, issued_date FROM giftcards WHERE id < 3"),
                clock=clock_2026).run()
    assert len(read_csv(r.csv)[2]) == 2
    assert r.manifest["dropped_by_default"] == []
