"""
The de-identification test vectors shared with the Java implementation
(java/hsm-crypto-client/src/test/resources/deid-test-vectors.json).

Passing here means a Databricks export gives a vendor the same tokens and finds the
same surrogate rows as the Java job. If this fails, fix the code, never the vectors.
"""

import json
import uuid
from pathlib import Path

import pytest

from hsm_databricks_udf.deid import keys, methods
from hsm_databricks_udf.deid.methods import FieldOptions, Normalize
from hsm_databricks_udf.deid.surrogate import SurrogateTransform

VECTORS = json.loads((Path(__file__).resolve().parents[3] / "java" / "hsm-crypto-client" / "src" / "test"
                      / "resources" / "deid-test-vectors.json").read_text(encoding="utf-8"))
DEK = bytes.fromhex(VECTORS["dek_hex"])
KEY_ID = uuid.UUID("6f1c0000-0000-4000-8000-000000000001")


def derived(label):
    return next(bytes.fromhex(h["key_hex"]) for h in VECTORS["hkdf"] if h["label"] == label)


def test_rfc5869_case3():
    c = VECTORS["rfc5869_case3"]
    assert keys.hkdf_sha256(bytes.fromhex(c["ikm_hex"]), bytes.fromhex(c["salt_hex"]),
                            bytes.fromhex(c["info_hex"]), c["length"]).hex() == c["okm_hex"]


@pytest.mark.parametrize("case", VECTORS["hkdf"], ids=lambda c: c["label"])
def test_hkdf_labels(case):
    assert keys.hkdf_sha256(DEK, None, case["label"].encode(), 32).hex() == case["key_hex"]


def test_the_label_constants_match_the_vectors():
    assert [h["label"] for h in VECTORS["hkdf"]] == [keys.TOKENIZE_LABEL, keys.SURROGATE_LOOKUP_LABEL,
                                                     keys.SURROGATE_ENC_LABEL]


@pytest.mark.parametrize("case", VECTORS["tokenize"], ids=lambda c: c["domain"] + ":" + c["normalize"])
def test_tokenize(case):
    t = methods.TokenizeTransform.create(FieldOptions({"normalize": case["normalize"]}),
                                         derived(keys.TOKENIZE_LABEL), case["domain"])
    assert t.apply(case["value"]) == case["token"]


@pytest.mark.parametrize("case", VECTORS["lookup_hash"], ids=lambda c: c["domain"] + ":" + c["value"])
def test_lookup_hash(case):
    s = SurrogateTransform.create(FieldOptions({"normalize": case["normalize"]}), case["domain"], "any-scope",
                                  (derived(keys.SURROGATE_LOOKUP_LABEL), derived(keys.SURROGATE_ENC_LABEL)), KEY_ID)
    assert s.lookup_hash(Normalize.from_label(case["normalize"]).apply(case["value"])).hex() == case["hash_hex"]


def test_the_key_provider_derives_the_vector_keys_with_one_unwrap():
    calls = []

    def unwrap(edek_id):
        calls.append(edek_id)
        return bytearray(DEK)

    with keys.KeyProvider(unwrap) as p:
        lookup, enc = p.keys(KEY_ID, *SurrogateTransform.key_labels())
        assert p.key(KEY_ID, keys.TOKENIZE_LABEL) == derived(keys.TOKENIZE_LABEL)
        assert lookup == derived(keys.SURROGATE_LOOKUP_LABEL)
        assert enc == derived(keys.SURROGATE_ENC_LABEL)
        assert p.key(KEY_ID, keys.TOKENIZE_LABEL) == derived(keys.TOKENIZE_LABEL)
    assert calls == [KEY_ID, KEY_ID], "one unwrap per call that needs a new label, none once cached"
