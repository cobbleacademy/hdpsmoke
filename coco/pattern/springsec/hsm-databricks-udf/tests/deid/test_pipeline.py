"""The per-batch pipeline in plain Python (no Spark): decrypt, transforms, surrogate inputs, counts, errors."""

import pytest

from hsm_databricks_udf.deid import parse_yaml
from hsm_databricks_udf.deid.errors import DeidError
from hsm_databricks_udf.deid.pipeline import BatchProcessor, BatchStats, columns_to_read, unmarked_ciphertext

from conftest import COLUMN_DEK_ID, FakeHsm, SURROGATE_KEY_ID, clock_2026

PROFILE = """
id: zen
version: 1
hipaa-basis: safe-harbor
surrogate-scope: zen-analytics
fields:
  - { name: member_id,   phi: member-id,   method: surrogate, domain: member }
  - { name: card_number, phi: card-number, decrypt: true, method: surrogate, domain: card, normalize: digits }
  - { name: dob,         phi: birth-date,  method: generalize, to: birth-year }
  - { name: zip,         phi: zip,         method: generalize, to: zip3 }
  - { name: email,       phi: email,       method: drop }
  - { name: amount,                        method: passthrough }
output: { file: "zen_{yyyyMMdd}.csv", rename: { member_id: member_ref } }
"""


def processor(yaml=PROFILE, hsm=None, key_column=None):
    return BatchProcessor(parse_yaml(yaml), hsm=hsm or FakeHsm(), clock=clock_2026,
                          surrogate_key_edek_id=SURROGATE_KEY_ID, key_column=key_column)


def test_a_batch():
    hsm = FakeHsm()
    p = processor(hsm=hsm)
    assert p.header == ["member_ref", "card_number", "dob", "zip", "amount"]
    assert p.surrogate_indexes() == [0, 1]
    records = [
        {"member_id": "M1234567", "card_number": hsm.encrypt("6035-7123-4567-8901"), "dob": "1985-03-14",
         "zip": "60614-2231", "amount": "25.00"},
        {"member_id": "M1234567 ", "card_number": hsm.encrypt("6035712345678901"), "dob": "1933-07-30",
         "zip": "03601", "amount": "-5.00"},
        {"member_id": "", "card_number": None, "dob": None, "zip": "", "amount": "1"},
    ]
    r = p.process(records)
    assert r.values[2] == ["1985", "<=1936", None]
    assert r.values[3] == ["606", "000", ""]
    assert r.values[4] == ["25.00", "-5.00", "1"]
    # surrogate fields: hashes for non-empty values (same member, same hash), empties kept
    assert r.hashes[0][0] == r.hashes[0][1] and r.hashes[0][2] is None
    assert r.values[0] == [None, None, ""]
    assert r.hashes[1][0] == r.hashes[1][1], "normalize: digits makes both card numbers one value"
    assert r.value_enc[0][0] is r.value_enc[0][1], "one value_enc per distinct value in a batch"
    assert r.stats.rows == 3
    assert r.stats.values == {0: 3, 1: 3, 2: 3, 3: 3, 4: 3}
    assert r.stats.empty == {0: 1, 1: 1, 2: 1, 3: 1, 4: 0}
    assert r.stats.counters[1] == {"decrypted": 2}
    assert r.stats.counters[2] == {"capped": 1}
    assert r.stats.counters[3] == {"restricted": 1}
    assert r.stats.edek_ids == {str(COLUMN_DEK_ID)}
    rows = p.rows(r, [{r.hashes[0][0]: "S-A"}, {r.hashes[1][0]: "S-C"}, None, None, None])
    assert rows == [["S-A", "S-C", "1985", "606", "25.00"], ["S-A", "S-C", "<=1936", "000", "-5.00"],
                    ["", None, None, "", "1"]]


def test_stats_add_up_and_travel_as_json():
    a = BatchStats(2, {0: 2}, {0: 1}, {0: {"capped": 1}}, {"x"})
    b = BatchStats.from_json_dict(BatchStats(3, {0: 3, 1: 3}, {}, {0: {"capped": 2, "restricted": 1}},
                                             {"y"}).to_json_dict())
    a.add(b)
    assert (a.rows, a.values, a.empty, a.counters, a.edek_ids) == (
        5, {0: 5, 1: 3}, {0: 1}, {0: {"capped": 3, "restricted": 1}}, {"x", "y"})


def test_errors_name_the_field_and_key_never_the_value():
    p = processor(key_column="id")
    with pytest.raises(DeidError) as e:
        p.process([{"id": "17", "member_id": "M1", "card_number": "6035712345678901", "dob": "1985-03-14",
                    "zip": "60614", "amount": "1"}])
    assert str(e.value).startswith("field 'card_number', id 17: marked decrypt: true but not an HSM ciphertext")
    assert "6035" not in str(e.value)
    with pytest.raises(DeidError) as e:
        p.process([{"id": "18", "member_id": "M1", "card_number": None, "dob": "14/03/1985", "zip": "60614",
                    "amount": "1"}])
    assert str(e.value) == "field 'dob', id 18: not an ISO date or date-time; set input-format"
    digits = processor(PROFILE.replace("domain: member }", "domain: member, normalize: digits }"))
    with pytest.raises(DeidError) as e:
        digits.process([{"member_id": "M-", "card_number": None}])
    assert str(e.value) == "field 'member_id': nothing left after normalize: digits"


def test_a_missing_grant_fails_naming_the_key():
    hsm = FakeHsm()
    token = hsm.encrypt("6035712345678901")
    no_grant = FakeHsm({k: v for k, v in hsm.deks.items() if k != COLUMN_DEK_ID})
    p = processor(hsm=no_grant)
    with pytest.raises(DeidError) as e:
        p.verify_decrypt([{"card_number": None}, {"card_number": token}])
    assert str(e.value) == ("field 'card_number': decrypt failed for edek_id=" + str(COLUMN_DEK_ID)
                            + ": dek/unwrap failed: no decrypt grant on edek_id")


def test_unmarked_ciphertext_and_columns_to_read():
    hsm = FakeHsm()
    profile = parse_yaml(PROFILE.replace("decrypt: true, ", ""))
    assert unmarked_ciphertext(profile, [{"card_number": hsm.encrypt("1")}, {"member_id": "M1"}]) == ["card_number"]
    assert unmarked_ciphertext(profile, [{"card_number": "v1.notatoken"}]) == []
    with_as_of = parse_yaml(PROFILE.replace("to: birth-year }", "to: birth-year, as-of: issued }")
                            .replace("safe-harbor", "minimum-necessary"))
    assert columns_to_read(with_as_of) == ["member_id", "card_number", "dob", "issued", "zip", "amount"]


def test_a_profile_with_only_drops_is_refused():
    with pytest.raises(RuntimeError, match="exports no fields"):
        processor("""
id: x
version: 1
hipaa-basis: minimum-necessary
fields: [ { name: a, method: drop } ]
output: { file: o.csv }
""")
