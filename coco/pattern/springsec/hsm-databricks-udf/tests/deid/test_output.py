"""CSV output, the manifest's layout, source value text and job helpers: the Java job's cases (CsvRecordSinkTest)."""

import datetime as dt
import hashlib

import pytest

from hsm_databricks_udf.deid import csv_output, manifest, source
from hsm_databricks_udf.deid.errors import DeidError, DeidValidationError
from hsm_databricks_udf.deid.job import manifest_name, output_file_name, safe_reason
from hsm_databricks_udf.deid import parse_yaml


def write(tmp_path, header, rows, delimiter=",", crlf=False, guard=True):
    part = tmp_path / "part-00000.txt"
    ending = "\r\n" if crlf else "\n"
    part.write_bytes("".join(csv_output.line(r, delimiter, guard) + ending for r in rows).encode("utf-8"))
    target = tmp_path / "out" / "out.csv"
    sha = csv_output.assemble(target, header, [part], delimiter=delimiter, crlf=crlf)
    return target, sha


def test_quotes_only_what_needs_quoting(tmp_path):
    target, _ = write(tmp_path, ["a", "b", "c"], [["plain", "with,comma", 'with "quote"'], [None, "", "two\nlines"]])
    assert target.read_text(encoding="utf-8") == 'a,b,c\nplain,"with,comma","with ""quote"""\n,,"two\nlines"\n'


def test_formula_guard_prefixes_a_quote(tmp_path):
    target, _ = write(tmp_path, ["v"], [["=SUM(A1)"], ["+1"], ["-5.00"], ["@cmd"], ["\tx"], ["ok"]])
    assert target.read_text(encoding="utf-8") == "v\n'=SUM(A1)\n'+1\n'-5.00\n'@cmd\n'\tx\nok\n"


def test_formula_guard_off_and_never_on_the_header(tmp_path):
    target, _ = write(tmp_path, ["-h"], [["-5.00"]], guard=False)
    assert target.read_text(encoding="utf-8") == "-h\n-5.00\n"


def test_crlf_and_another_delimiter(tmp_path):
    target, _ = write(tmp_path, ["a", "b"], [["x,y", "p|q"]], delimiter="|", crlf=True)
    assert target.read_bytes() == b'a|b\r\nx,y|"p|q"\r\n'


def test_sha256_part_file_and_no_overwrite(tmp_path):
    target, sha = write(tmp_path, ["name"], [["Zoë"]])
    data = target.read_bytes()
    assert data[0:1] == b"n", "no byte order mark"
    assert sha == hashlib.sha256(data).hexdigest()
    assert not (target.parent / "out.csv.part").exists()
    with pytest.raises(RuntimeError, match="already exists; not overwriting it"):
        csv_output.assemble(target, ["x"], [], delimiter=",", crlf=False)
    assert target.read_bytes() == data


def test_a_failed_assemble_leaves_nothing(tmp_path):
    with pytest.raises(FileNotFoundError):
        csv_output.assemble(tmp_path / "o.csv", ["a"], [tmp_path / "missing.txt"], delimiter=",", crlf=False)
    assert list(tmp_path.iterdir()) == []


def test_manifest_layout_matches_jackson():
    m = {"run_id": "r", "profile": {"id": "zen", "version": 1}, "empty": {}, "fields": [{"source": "a", "n": 2},
         {"source": "b"}], "list": ["x", "y"], "none": [], "null": None, "flag": True}
    assert manifest.to_json(m) == (
        '{\n  "run_id" : "r",\n  "profile" : {\n    "id" : "zen",\n    "version" : 1\n  },\n  "empty" : { },\n'
        '  "fields" : [ {\n    "source" : "a",\n    "n" : 2\n  }, {\n    "source" : "b"\n  } ],\n'
        '  "list" : [ "x", "y" ],\n  "none" : [ ],\n  "null" : null,\n  "flag" : true\n}')


def test_instants_as_java_prints_them():
    utc = dt.timezone.utc
    assert manifest.instant(dt.datetime(2026, 11, 20, 15, 30, tzinfo=utc)) == "2026-11-20T15:30:00Z"
    assert manifest.instant(dt.datetime(2026, 11, 20, 15, 30, 1, 500000, tzinfo=utc)) == "2026-11-20T15:30:01.500Z"
    assert manifest.instant(dt.datetime(2026, 11, 20, 15, 30, 1, 123456, tzinfo=utc)) == "2026-11-20T15:30:01.123456Z"
    assert manifest.instant(dt.datetime(2026, 11, 20, 10, 30, tzinfo=dt.timezone(dt.timedelta(hours=-5)))) \
        == "2026-11-20T15:30:00Z"


@pytest.mark.parametrize("spark_text,kind,java", [
    ("2026-11-02 10:15:00", "timestamp", "2026-11-02T10:15"),
    ("2026-11-02 10:15:07", "timestamp", "2026-11-02T10:15:07"),
    ("2026-11-02 10:15:00.5", "timestamp", "2026-11-02T10:15:00.500"),
    ("2026-11-02 10:15:00.123456", "timestamp", "2026-11-02T10:15:00.123456"),
    ("2026-11-02 00:00:00.000001", "timestamp", "2026-11-02T00:00:00.000001"),
    ("25.00", "decimal", "25.00"),
    ("1E-7", "decimal", "0.0000001"),
    ("-0.50", "decimal", "-0.50"),
    ("abc", "plain", "abc"),
    (None, "decimal", None),
])
def test_values_as_the_java_jdbc_source_writes_them(spark_text, kind, java):
    assert source.to_java_string(spark_text, kind) == java


def test_binary_and_complex_columns_are_refused():
    assert source.check_columns([("ID", "int"), ("Amount", "decimal(10,2)"), ("t", "timestamp_ntz")],
                                ["id", "amount", "t", "missing"]) == {
        "id": ("ID", "plain"), "amount": ("Amount", "decimal"), "t": ("t", "timestamp")}
    with pytest.raises(DeidError, match="column 'photo' is binary; it can't be written to a CSV"):
        source.check_columns([("photo", "binary")], ["photo"])
    with pytest.raises(DeidError, match="column 'tags' is a array"):
        source.check_columns([("tags", "array<string>")], ["tags"])


def test_output_file_names_as_the_java_job():
    p = parse_yaml("id: zen\nversion: 1\nhipaa-basis: minimum-necessary\nfields: [ {name: a, method: passthrough} ]\n"
                   "output: { file: \"{profile}_{yyyyMMdd}_{run}.csv\" }\n")
    t = dt.datetime(2026, 11, 20, 23, 30, tzinfo=dt.timezone(dt.timedelta(hours=-5)))
    assert output_file_name(p, "deid-x", t) == "zen_20261121_deid-x.csv", "the date is taken in UTC"
    assert manifest_name("zen.CSV") == "zen.manifest.json"
    assert manifest_name("zen.txt") == "zen.txt.manifest.json"
    bad = parse_yaml("id: zen\nversion: 1\nhipaa-basis: minimum-necessary\nfields: [ {name: a, method: passthrough} ]\n"
                     "output: { file: \"../x.csv\" }\n")
    with pytest.raises(RuntimeError, match="plain file name"):
        output_file_name(bad, "r", t)


def test_safe_reason():
    assert safe_reason(DeidValidationError(["a", "b"])) == "2 profile problem(s)"
    assert safe_reason(DeidError("field 'x': bad")) == "field 'x': bad"
    assert safe_reason(Exception("pyspark... DeidError: field 'x', id 3: not a date\n  at ...")) == \
        "field 'x', id 3: not a date"
    assert safe_reason(KeyError("M1234567")) == "KeyError"
