"""
The profiles in java/hsm-crypto-client/src/test/resources/deid-profile-cases.json:
what the Java job's YAML reading, DeidProfile.fromMap and ProfileValidator give
(problems in order, output column names), which the Java test DeidProfileCasesTest
checks too. Passing both means a profile that runs in one runs in the other, and
is refused with the same messages. The example profiles are among the cases.
"""

import json
from pathlib import Path

import pytest

from hsm_databricks_udf.deid import DeidValidationError, parse_yaml, problems
from hsm_databricks_udf.deid.methods import FieldOptions, GeneralizeTransform

DOC = json.loads((Path(__file__).resolve().parents[3] / "java" / "hsm-crypto-client" / "src" / "test" / "resources"
                  / "deid-profile-cases.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", DOC["cases"], ids=lambda c: str(DOC["cases"].index(c)))
def test_case(case):
    got = {}
    try:
        p = parse_yaml(case["yaml"])
        got["problems"] = problems(p, case["columns"])
        got["outputs"] = [p.output_name(f) for f in p.fields]
    except DeidValidationError as e:
        got["structural"] = e.problems
    expected = {k: case[k] for k in ("problems", "outputs", "structural") if k in case}
    assert got == expected


def test_the_example_profiles_are_cases():
    examples = sorted(p.name for p in (Path(__file__).resolve().parents[3] / "java" / "hsm-bulk-client"
                                       / "config-examples" / "deid-profiles").glob("*.yml"))
    yamls = {c["yaml"] for c in DOC["cases"]}
    for name in examples:
        text = (Path(__file__).resolve().parents[3] / "java" / "hsm-bulk-client" / "config-examples"
                / "deid-profiles" / name).read_text(encoding="utf-8")
        assert text in yamls, name + " changed: regenerate deid-profile-cases.json from the Java side"


def test_date_pattern_letters_python_cant_read_are_refused():
    # The one deliberate difference: Java accepts any DateTimeFormatter letter; the
    # Python job refuses the profile rather than read such a date differently.
    with pytest.raises(ValueError, match="pattern letter"):
        GeneralizeTransform.create(FieldOptions({"to": "year", "input-format": "EEE yyyy"}))
