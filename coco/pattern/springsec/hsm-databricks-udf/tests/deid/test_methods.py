"""
mask, generalize, tokenize, normalize and the surrogate primitives: the cases of the
Java tests (MaskTransformTest, GeneralizeTransformTest, SurrogateTransformTest) with
the same expected values, plus the Java string rules the port depends on.
"""

import datetime as dt
import re
import uuid

import pytest

from hsm_databricks_udf.deid import _java, keys
from hsm_databricks_udf.deid.errors import DeidError
from hsm_databricks_udf.deid.methods import (FieldOptions, FieldStats, GeneralizeTransform, JavaDatePattern,
                                             MaskTransform, Normalize, iso_year)
from hsm_databricks_udf.deid.surrogate import CROCKFORD, SurrogateTransform

CLOCK_2026 = lambda: dt.datetime(2026, 11, 20, 15, 30, tzinfo=dt.timezone.utc)  # noqa: E731
DEK = bytes(range(32))
SURROGATE_KEYS = (keys.hkdf_sha256(DEK, None, keys.SURROGATE_LOOKUP_LABEL.encode(), 32),
                  keys.hkdf_sha256(DEK, None, keys.SURROGATE_ENC_LABEL.encode(), 32))


def mask(options, value):
    return MaskTransform.create(FieldOptions(options)).apply(value)


def gen(options, clock=CLOCK_2026):
    return GeneralizeTransform.create(FieldOptions(options), clock)


def run(options, value, record=None, stats=None):
    return gen(options).apply(value, record or {}, stats or FieldStats())


# ---------------------------------------------------------------- mask

def test_keep_last_shows_the_last_digits_of_a_card():
    assert mask({"keep-last": 4}, "6035712345678901") == "************8901"


def test_email_keeps_the_domain_and_the_first_character():
    o = {"keep": "domain", "keep-first": 1}
    assert mask(o, "jane.doe@mail.com") == "j*******@mail.com"
    assert mask(o, "raj.p@corp.org") == "r****@corp.org"
    assert mask(o, "not-email!") == "**********"
    assert mask(o, "ab@x@host.com") == "a***@host.com"


def test_preserve_leaves_separators_in_place():
    assert mask({"keep-last": 4, "preserve": ["-"]}, "6035-7123-4567-8901") == "****-****-****-8901"
    assert mask({"keep-last": 4, "preserve": ["-"]}, "312-555-0142") == "***-***-0142"


def test_no_options_masks_everything():
    assert mask({}, "Jane Doe") == "********"


def test_fixed_length_hides_the_real_length():
    assert mask({"fixed-length": 4}, "4821") == "****"
    assert mask({"fixed-length": 4}, "123456789") == "****"
    assert mask({"fixed-length": 8, "keep-last": 4}, "6035712345678901") == "********8901"


def test_a_value_shorter_than_the_kept_parts_is_masked_in_full():
    assert mask({"keep-first": 2, "keep-last": 2}, "abcd") == "****"
    assert mask({"keep-last": 4}, "123") == "***"


def test_custom_character_and_code_points():
    assert mask({"char": "#", "keep-last": 1}, "Zoë") == "##ë"
    assert mask({"char": "•", "keep-last": 1}, "1234") == "•••4"
    assert mask({"keep-last": 1}, "a😀b") == "**b", "an emoji is one character, as Java code points"


def test_mask_null_and_empty_pass_through():
    assert mask({"keep-last": 4}, None) is None
    assert mask({"keep-last": 4}, "") == ""


@pytest.mark.parametrize("options", [{"keep-lst": 4}, {"keep": "local"}, {"keep": "domain", "keep-last": 2},
                                     {"char": "**"}, {"keep-last": -1}, {"keep-last": True}, {"keep-last": 4.5}])
def test_bad_mask_options_are_rejected(options):
    with pytest.raises(ValueError):
        mask(options, "x")


def test_fixed_length_with_preserve_names_the_option():
    with pytest.raises(ValueError, match="fixed-length"):
        mask({"fixed-length": 4, "preserve": ["-"]}, "x")


def test_a_whole_float_is_a_whole_number_as_in_java():
    assert mask({"keep-last": 4.0}, "6035712345678901") == "************8901"
    assert mask({"keep-last": "4"}, "6035712345678901") == "************8901"


# ---------------------------------------------------------------- generalize

def test_year_from_dates_and_date_times():
    o = {"to": "year"}
    assert run(o, "2026-10-14") == "2026"
    assert run(o, "2026-10-14T09:15:00") == "2026"
    assert run(o, "2026-10-14T09:15") == "2026"
    assert run(o, "2026-10-14t09:15:00.123456789") == "2026"
    assert run(o, "2026-10-14T09:15:00+05:30") == "2026"
    assert run(o, "2026-10-14T09:15:00+05") == "2026"
    assert run(o, "2025-12-31T23:59:59Z") == "2025"
    assert run(o, "2025-12-31T23:00:00-05:00") == "2025", "the offset date-time's own year, not UTC"
    assert run({"to": "year", "input-format": "MM/dd/yyyy"}, "03/12/1958") == "1958"


def test_birth_year_caps_ages_over_89():
    o = {"to": "birth-year"}
    assert run(o, "1958-03-12") == "1958"
    assert run(o, "1937-01-01") == "1937"
    assert run(o, "1936-12-31") == "<=1936"
    assert run(o, "1933-07-30") == "<=1936"
    assert run(o, "1901-01-01") == "<=1936"


def test_birth_year_cutoff_moves_at_the_year_boundary():
    o = {"to": "birth-year"}
    last_day_2026 = lambda: dt.datetime(2026, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc)  # noqa: E731
    first_day_2027 = lambda: dt.datetime(2027, 1, 1, tzinfo=dt.timezone.utc)  # noqa: E731
    assert gen(o, last_day_2026).apply("1937-06-01", {}, FieldStats()) == "1937"
    assert gen(o, first_day_2027).apply("1937-06-01", {}, FieldStats()) == "<=1937"
    eastern_new_year = lambda: dt.datetime(2027, 1, 1, 1, tzinfo=dt.timezone(dt.timedelta(hours=5)))  # noqa: E731
    assert gen(o, eastern_new_year).run_year() == 2026, "the run's year is taken in UTC"


def test_birth_year_options_and_count():
    stats = FieldStats()
    t = gen({"to": "birth-year", "over-89-as": "90+"})
    assert t.apply("1933-07-30", {}, stats) == "90+"
    assert gen({"to": "birth-year", "over-89-as": ""}).apply("1933-07-30", {}, stats) == ""
    assert stats.counter("capped") == 2
    assert t.describe()["cutoff_year"] == 1936


def test_birth_year_as_of_another_field():
    t = gen({"to": "birth-year", "as-of": "issued"})
    assert t.apply("1933-07-30", {"issued": "2020-05-01"}, FieldStats()) == "1933"
    assert t.apply("1929-01-01", {"issued": "2020-05-01"}, FieldStats()) == "<=1930"
    assert t.apply("1933-07-30", {}, FieldStats()) == "<=1936"
    with pytest.raises(DeidError, match="as-of"):
        t.apply("1933-07-30", {"issued": "soon"}, FieldStats())


def test_zip3_with_restricted_areas_as_zeros():
    stats = FieldStats()
    t = gen({"to": "zip3"})
    assert t.apply("60614", {}, stats) == "606"
    assert t.apply("94107-1234", {}, stats) == "941"
    assert t.apply("941071234", {}, stats) == "941"
    assert t.apply(" 60614 ", {}, stats) == "606"
    assert t.apply("03601", {}, stats) == "000"
    assert t.apply("89301", {}, stats) == "000"
    assert stats.counter("restricted") == 2
    for bad in ("6061", "ABCDE", "６０６１４"):
        with pytest.raises(DeidError):
            t.apply(bad, {}, stats)


def test_ages_cap_at_90():
    assert run({"to": "age-90"}, "89") == "89"
    assert run({"to": "age-90"}, "90") == "90+"
    assert run({"to": "age-90"}, "92") == "90+"
    assert run({"to": "age-90"}, "+5") == "5", "Integer.parseInt takes a + sign"
    assert run({"to": "age-band"}, "43") == "40-49"
    assert run({"to": "age-band", "width": 5}, "85") == "85-89"
    assert run({"to": "age-band", "width": 7}, "88") == "84-89"
    assert run({"to": "age-band"}, "95") == "90+"
    for bad in ("-1", "forty", "1_0", "151"):
        with pytest.raises(DeidError):
            run({"to": "age-90"}, bad)


def test_unparseable_values_fail_without_the_value():
    with pytest.raises(DeidError) as e:
        run({"to": "year"}, "12th March 1958")
    assert "1958" not in str(e.value)


@pytest.mark.parametrize("value", ["2026-02-30", "2026-13-01", "20261014", "2026-10-14 09:15", "2026-10-14T24:00",
                                   "2026-1-14", "+2026-10-14", "2026-10-14T09:15:00+19:00"])
def test_what_java_time_refuses_is_refused(value):
    assert iso_year(value) is None


def test_leap_seconds_only_as_an_instant():
    assert iso_year("2025-12-31T23:59:60Z") == 2025
    assert iso_year("2025-12-31T23:59:60-01:00") == 2026, "Instant.parse converts to UTC"
    assert iso_year("2025-12-31T23:59:60") is None


def test_generalize_null_and_empty_pass_through():
    assert run({"to": "year"}, None) is None
    assert run({"to": "zip3"}, "") == ""


@pytest.mark.parametrize("options", [{}, {"to": "month"}, {"to": "zip3", "width": 5}, {"to": "year", "as-of": "x"},
                                     {"to": "year", "input-format": "EEE dd MMM yyyy"},
                                     {"to": "year", "input-format": "d MMMM yyyy"}])
def test_bad_generalize_options_are_rejected(options):
    with pytest.raises(ValueError):
        gen(options)


@pytest.mark.parametrize("pattern,value,year", [
    ("MM/dd/yyyy", "03/12/1958", 1958),
    ("yyyyMMdd", "19580312", 1958),
    ("dd.MM.yy", "12.03.58", 2058),
    ("d MMM uuuu", "1 Mar 1958", 1958),
    ("yyyy-MM-dd'T'HH:mm", "1958-03-12T07:05", 1958),
    ("MM/dd/yyyy hh:mm a", "03/12/1958 07:05 PM", 1958),
    ("yyyy-DDD", "1958-071", 1958),
    ("''yyyy''", "'1958'", 1958),
])
def test_input_format_patterns(pattern, value, year):
    assert JavaDatePattern(pattern).year(value) == year


@pytest.mark.parametrize("pattern,value", [
    ("MM/dd/yyyy", "13/12/1958"),
    ("MM/dd/yyyy", "3/12/1958"),
    ("MM/dd/yyyy", "03/32/1958"),
    ("MMM dd yyyy", "mar 12 1958"),
    ("yyyy", "0000"),
    ("yyyy", "19580"),
    ("MM/dd", "03/12"),
])
def test_input_format_refusals(pattern, value):
    assert JavaDatePattern(pattern).year(value) is None


# ---------------------------------------------------------------- normalize and Java strings

def test_normalize_uses_java_whitespace_rules():
    assert Normalize.TRIM.apply(" M1234567\t") == "M1234567"
    assert Normalize.TRIM.apply(" M1 ") == " M1 ", "Java strip keeps the no-break space"
    assert Normalize.TRIM.apply(" M1 ") == "M1"
    assert Normalize.LOWER.apply(" Jane.Doe@Mail.COM ") == "jane.doe@mail.com"
    assert Normalize.DIGITS.apply("6035-7123 4567/8901") == "6035712345678901"
    assert Normalize.DIGITS.apply("٦٠٣٥1") == "1", "only ASCII digits, as Java's [^0-9]"
    with pytest.raises(DeidError, match="nothing left after normalize: digits"):
        Normalize.DIGITS.apply("--")


def test_java_value_of():
    assert _java.to_string(True) == "true"
    assert _java.to_string(4) == "4"
    assert _java.to_string(4.0) == "4.0"
    assert _java.to_string(1e7) == "1.0E7"
    assert _java.to_string(0.0001) == "1.0E-4"
    assert _java.trim(" x\u0001 ") == " x"


# ---------------------------------------------------------------- surrogate primitives

# Made by the Java SurrogateTransform.encryptValue with the vector DEK, scope
# zen-analytics, domain member: rows the Java job wrote can be mapped back here.
JAVA_VALUE_ENC = "d7694bed11da807e23cca864515e0ace5527c587739fe10b7d3a5926ebf883ddd26fe817"


def surrogate(scope="zen-analytics", domain="member", options=None):
    return SurrogateTransform.create(FieldOptions(options or {}), domain, scope, SURROGATE_KEYS,
                                     uuid.UUID("6f1c0000-0000-4000-8000-000000000003"))


def test_a_value_enc_from_the_java_job_maps_back():
    assert surrogate().decrypt_value(bytes.fromhex(JAVA_VALUE_ENC)) == "M1234567"


def test_value_enc_is_bound_to_scope_and_domain():
    enc = surrogate().encrypt_value("M1234567")
    assert surrogate().decrypt_value(enc) == "M1234567"
    assert enc != surrogate().encrypt_value("M1234567"), "a fresh IV each time"
    for other in (surrogate(scope="acme"), surrogate(domain="card")):
        with pytest.raises(DeidError, match="failed authentication"):
            other.decrypt_value(enc)
    with pytest.raises(DeidError, match="too short"):
        surrogate().decrypt_value(b"short")


def test_new_surrogates_have_the_java_format():
    s = surrogate()
    values = {s.new_surrogate() for _ in range(200)}
    assert len(values) == 200
    assert all(re.fullmatch("S-[" + CROCKFORD + "]{5}-[" + CROCKFORD + "]{5}", v) for v in values)
    long = surrogate(options={"length": 12, "prefix": "Z"}).new_surrogate()
    assert re.fullmatch("Z[0-9A-Z]{5}-[0-9A-Z]{5}-[0-9A-Z]{2}", long)


@pytest.mark.parametrize("options,domain", [({}, None), ({}, " "), ({"length": 7}, "member"),
                                            ({"normalize": "upper"}, "member"), ({"keep-last": 4}, "member")])
def test_bad_surrogate_options_are_rejected(options, domain):
    with pytest.raises(ValueError):
        SurrogateTransform.create(FieldOptions(options), domain, "s", SURROGATE_KEYS, None)
