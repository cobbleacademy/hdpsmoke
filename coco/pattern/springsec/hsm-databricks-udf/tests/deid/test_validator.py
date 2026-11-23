"""
Profile parsing and the validator: the cases of the Java ProfileValidatorTest, with
the same messages.
"""

import pytest

from hsm_databricks_udf.deid import DeidValidationError, parse_yaml, problems, validate

SOURCE = ["member_id", "first_name", "email", "phone", "dob", "zip", "card_number", "card_pin", "program",
          "card_type", "amount", "status", "issued_date", "age", "shipping_zip"]


def header(basis):
    return f"""
id: test
version: 1
hipaa-basis: {basis}
token-key-edek-id: 6f1c0000-0000-4000-8000-000000000001
token-domain: member
surrogate-scope: test-vendor
output: {{ file: "out.csv" }}
fields:
"""


def found(basis, fields):
    return problems(parse_yaml(header(basis) + fields), SOURCE)


def assert_problem(found_problems, fragment):
    assert any(fragment in p for p in found_problems), fragment + " not in " + str(found_problems)


def test_a_good_safe_harbor_profile_passes():
    assert found("safe-harbor", """
  - { name: member_id,   phi: member-id,   method: surrogate, domain: member }
  - { name: card_number, phi: card-number, method: surrogate, domain: card, decrypt: true }
  - { name: dob,         phi: birth-date,  method: generalize, to: birth-year }
  - { name: issued_date, phi: date,        method: generalize, to: year }
  - { name: zip,         phi: zip,         method: generalize, to: zip3 }
  - { name: age,         phi: age,         method: generalize, to: age-band, width: 10 }
  - { name: email,       phi: email,       method: drop }
  - { name: amount,                        method: passthrough }
""") == []


def test_safe_harbor_rejects_each_disallowed_method():
    p = found("safe-harbor", """
  - { name: member_id,   phi: member-id,  method: tokenize }
  - { name: email,       phi: email,      method: mask, keep: domain }
  - { name: first_name,  phi: name,       method: passthrough }
  - { name: dob,         phi: birth-date, method: generalize, to: year }
  - { name: issued_date, phi: date,       method: passthrough }
  - { name: zip,         phi: zip,        method: mask, keep-first: 3 }
  - { name: age,         phi: age,        method: passthrough }
""")
    assert_problem(p, "field 'member_id' (phi: member-id): safe-harbor allows only drop or surrogate (a token is derived")
    assert_problem(p, "field 'email' (phi: email): safe-harbor allows only drop")
    assert_problem(p, "field 'first_name' (phi: name): safe-harbor allows only drop")
    assert_problem(p, "field 'dob' (phi: birth-date): safe-harbor allows only drop or generalize to: birth-year")
    assert_problem(p, "field 'issued_date' (phi: date): safe-harbor allows only drop or generalize to: year")
    assert_problem(p, "field 'zip' (phi: zip): safe-harbor allows only drop or generalize to: zip3")
    assert_problem(p, "field 'age' (phi: age)")
    assert len(p) == 7, p


def test_safe_harbor_rejects_birth_year_as_of():
    assert_problem(found("safe-harbor", """
  - { name: dob, phi: birth-date, method: generalize, to: birth-year, as-of: issued_date }
"""), "safe-harbor doesn't allow as-of")


def test_minimum_necessary_allows_masking_identifiers():
    assert found("minimum-necessary", """
  - { name: member_id,   phi: member-id,   method: tokenize }
  - { name: first_name,  phi: name,        method: passthrough }
  - { name: email,       phi: email,       method: mask, keep: domain, keep-first: 1, decrypt: true }
  - { name: card_number, phi: card-number, method: mask, keep-last: 4, decrypt: true }
""") == []


def test_never_export_fields_may_only_be_dropped():
    assert_problem(found("minimum-necessary", """
  - { name: card_pin, method: mask }
  - { name: amount,   method: passthrough }
"""), "field 'card_pin' is a never-export field")
    assert found("minimum-necessary", """
  - { name: card_pin,     method: drop }
  - { name: shipping_zip, method: passthrough }
""") == [], "shipping_zip contains 'pin' but isn't a PIN"


def test_json_paths_and_never_export():
    p = problems(parse_yaml(header("minimum-necessary") + """
  - { name: "cards[].pin",    method: passthrough }
  - { name: "cards[].amount", method: passthrough }
"""), None)
    assert p == ["field 'cards[].pin' is a never-export field (PIN, CVV, activation or security code): "
                 "only drop is allowed",
                 "field 'cards[].pin': output column 'cards.pin' looks like a never-export field; rename it"]


def test_an_output_name_that_looks_like_a_pin_is_rejected():
    yaml = header("minimum-necessary").replace('output: { file: "out.csv" }',
                                              'output: { file: "out.csv", rename: { amount: card_pin } }') + """
  - { name: amount, method: passthrough }
"""
    assert_problem(problems(parse_yaml(yaml), SOURCE), "looks like a never-export field")


def test_decrypt_with_drop_is_rejected():
    assert_problem(found("minimum-necessary", """
  - { name: card_number, method: drop, decrypt: true }
  - { name: amount,      method: passthrough }
"""), "decrypt: true with drop")


def test_source_and_structure_checks():
    p = found("minimum-necessary", """
  - { name: amount,   method: passthrough }
  - { name: amount,   method: passthrough }
  - { name: missing,  method: passthrough }
  - { name: dob,      method: generalize, to: birth-year, as-of: no_such_field }
  - { name: phone,    method: mask, keep-lst: 4 }
""")
    assert_problem(p, "field 'amount' is listed more than once")
    assert_problem(p, "field 'missing' is not in the source")
    assert_problem(p, "as-of field 'no_such_field' is not in the source")
    assert_problem(p, "field 'phone': unknown option(s) for mask: keep-lst")


def test_keys_and_scope_are_required_for_their_methods():
    p = problems(parse_yaml("""
id: test
version: 1
hipaa-basis: minimum-necessary
output: { file: "out.csv" }
fields:
  - { name: member_id,   method: tokenize }
  - { name: card_number, method: surrogate, domain: card }
"""), SOURCE)
    assert_problem(p, "tokenize needs token-key-edek-id")
    assert_problem(p, "tokenize needs a domain")
    assert_problem(p, "surrogate needs surrogate-scope")


def test_expert_determination_needs_a_report():
    assert_problem(found("expert-determination", """
  - { name: amount, method: passthrough }
"""), "expert-report")


def test_structural_problems_are_all_reported_at_once():
    with pytest.raises(DeidValidationError) as e:
        parse_yaml("""
id: x
hipaa-basis: safe-habor
colour: blue
fields:
  - { name: a, method: scramble }
  - { method: drop }
output: { file: out.csv, line-ending: cr }
""")
    p = e.value.problems
    for fragment in ("version is required", "unknown hipaa-basis 'safe-habor'", "unknown key(s) in profile: colour",
                     "unknown method 'scramble'", "name is required", "output.line-ending must be lf or crlf"):
        assert_problem(p, fragment)


def test_validate_raises_with_every_problem():
    with pytest.raises(DeidValidationError) as e:
        validate(parse_yaml(header("safe-harbor") + """
  - { name: first_name, phi: name, method: passthrough }
  - { name: email,      phi: email, method: passthrough }
"""), SOURCE)
    assert len(e.value.problems) == 2


def test_hipaa_basis_is_required():
    with pytest.raises(DeidValidationError) as e:
        parse_yaml("""
id: x
version: 1
fields: [ { name: a, method: drop } ]
output: { file: out.csv }
""")
    assert e.value.problems == ["hipaa-basis is required"]


def test_invalid_yaml_names_the_place_only():
    with pytest.raises(DeidValidationError) as e:
        parse_yaml("id: x\nfields: [ { name: cards[].pin } ]\n")
    assert e.value.problems[0].startswith("the profile is not valid YAML at line 2")


def test_output_names_drop_the_array_marker_unless_renamed():
    p = parse_yaml(header("minimum-necessary").replace('output: { file: "out.csv" }',
                                                       'output: { file: "out.csv", rename: { "cards[].sku": sku } }')
                   + """
  - { name: "cards[].amount", method: passthrough }
  - { name: "cards[].sku",    method: passthrough }
""")
    assert [p.output_name(f) for f in p.fields] == ["cards.amount", "sku"]
