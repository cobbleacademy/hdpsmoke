"""
The edge cases in java/hsm-crypto-client/src/test/resources/deid-method-cases.json:
outputs (and error messages) the Java mask, generalize and normalize give, which the
Java test DeidMethodCasesTest checks too. Passing both means a Databricks export
masks and generalizes exactly as the Java job. Fix the code, never the cases.
"""

import datetime as dt
import json
from pathlib import Path

import pytest

from hsm_databricks_udf.deid.errors import DeidError
from hsm_databricks_udf.deid.methods import FieldOptions, FieldStats, GeneralizeTransform, MaskTransform, Normalize

DOC = json.loads((Path(__file__).resolve().parents[3] / "java" / "hsm-crypto-client" / "src" / "test" / "resources"
                  / "deid-method-cases.json").read_text(encoding="utf-8"))
RUN_TIME = dt.datetime.fromisoformat(DOC["clock"].replace("Z", "+00:00"))


def _run(case):
    o, v = case["options"], case["value"]
    if case["method"] == "generalize":
        return GeneralizeTransform.create(FieldOptions(o), lambda: RUN_TIME).apply(v, {}, FieldStats())
    if case["method"] == "mask":
        return MaskTransform.create(FieldOptions(o)).apply(v)
    return Normalize.from_label(o["normalize"]).apply(v)


@pytest.mark.parametrize("case", DOC["cases"], ids=lambda c: c["method"] + ":" + json.dumps(c["options"]) + ":" + c["value"])
def test_case(case):
    try:
        got = {"out": _run(case)}
    except (DeidError, ValueError) as e:
        got = {"err": str(e)}
    expected = {k: case[k] for k in ("out", "err") if k in case}
    assert got == expected


def test_full_month_names_are_refused_up_front():
    # The one deliberate difference: Java accepts MMMM but, in Locale.ROOT, matches no
    # real month name, so every value fails mid-run. Python refuses the profile instead.
    with pytest.raises(ValueError, match="MMMM"):
        GeneralizeTransform.create(FieldOptions({"to": "year", "input-format": "d MMMM yyyy"}))
