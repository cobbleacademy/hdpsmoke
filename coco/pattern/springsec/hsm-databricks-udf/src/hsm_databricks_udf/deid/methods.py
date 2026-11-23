"""
The de-identification methods (java/docs/DEIDENTIFICATION.md section 4), as the
Java transforms in com.hsm.client.deid.transform: drop, passthrough, mask,
generalize and tokenize here; surrogate in ``surrogate.py``.

Each transform has ``apply(value, record, stats)``: null in gives null out, an empty
string stays empty, and a value that can't be transformed raises ``DeidError`` with
a reason that never contains the value. ``create`` builds a field's transform;
``check_options`` checks a field's options without keys (for the validator).

Option errors are ``ValueError``s without the field name; the caller adds it.
"""

from __future__ import annotations

import base64
import datetime as _dt
import enum
import re
from importlib import resources

from . import _java
from .errors import DeidError
from .keys import TOKENIZE_LABEL, KeyProvider, hmac_sha256
from .profile import DeidProfile, Field, Method


# ---------------------------------------------------------------- options and stats

class FieldOptions:
    """A field's options as written in the profile, read with types; misspelt options are refused."""

    def __init__(self, raw: dict | None):
        self._raw = raw or {}
        self._read: set[str] = set()

    def has(self, key: str) -> bool:
        self._read.add(key)
        return self._raw.get(key) is not None

    def string(self, key: str, default: str | None) -> str | None:
        self._read.add(key)
        v = self._raw.get(key)
        if v is None:
            return default
        if isinstance(v, (dict, list)):
            raise ValueError("option '" + key + "' must be a single value")
        return _java.to_string(v)

    def integer(self, key: str, default: int, lo: int, hi: int) -> int:
        self._read.add(key)
        v = self._raw.get(key)
        if v is None:
            return default
        if isinstance(v, (int, float)) and not isinstance(v, bool) and float(v).is_integer():
            n = int(v)
        else:
            try:
                n = _java.parse_int(_java.trim(_java.to_string(v)))
            except ValueError:
                raise ValueError("option '" + key + "' must be a whole number") from None
        if n < lo or n > hi:
            raise ValueError("option '" + key + "' must be between " + str(lo) + " and " + str(hi))
        return n

    def string_list(self, key: str) -> list[str]:
        self._read.add(key)
        v = self._raw.get(key)
        if v is None:
            return []
        if not isinstance(v, list):
            raise ValueError("option '" + key + "' must be a list, e.g. [\"-\"]")
        return [_java.to_string(o) for o in v]

    def reject_unknown(self, method: str) -> None:
        unknown = sorted(set(self._raw.keys()) - self._read)
        if unknown:
            raise ValueError("unknown option(s) for " + method + ": " + ", ".join(unknown))


class FieldStats:
    """Counts for one output field (values, empty values, method counters). Never values."""

    def __init__(self):
        self.values = 0
        self.empty = 0
        self.counters: dict[str, int] = {}

    def count_value(self, value: str | None) -> None:
        self.values += 1
        if not value:
            self.empty += 1

    def increment(self, counter: str) -> None:
        self.add(counter, 1)

    def add(self, counter: str, n: int) -> None:
        self.counters[counter] = self.counters.get(counter, 0) + n

    def counter(self, name: str) -> int:
        return self.counters.get(name, 0)


# ---------------------------------------------------------------- normalize

class Normalize(enum.Enum):
    """How a value is normalized before tokenize or a surrogate lookup. ``trim`` by default."""

    NONE = "none"
    TRIM = "trim"
    LOWER = "lower"
    DIGITS = "digits"

    @property
    def label(self) -> str:
        return self.value

    @classmethod
    def from_label(cls, label: str) -> "Normalize":
        for n in cls:
            if n.value == label:
                return n
        raise ValueError("option 'normalize' must be one of none, trim, lower, digits")

    def apply(self, value: str) -> str:
        if self is Normalize.NONE:
            out = value
        elif self is Normalize.TRIM:
            out = _java.strip(value)
        elif self is Normalize.LOWER:
            out = _java.strip(value).lower()
        else:
            out = re.sub(r"[^0-9]", "", value)
        if not out:
            raise DeidError.bad_value("nothing left after normalize: " + self.label)
        return out


def domain_prefix(domain: str) -> bytes:
    """``domain ‖ 0x00``, the HMAC input prefix shared by tokenize and surrogate lookups."""
    return domain.encode("utf-8") + b"\x00"


# ---------------------------------------------------------------- drop, passthrough

class DropTransform:
    method = Method.DROP

    @staticmethod
    def check_options(o: FieldOptions) -> None:
        o.reject_unknown("drop")

    def apply(self, value, record=None, stats=None):
        return None

    def describe(self) -> dict:
        return {}


class PassthroughTransform:
    method = Method.PASSTHROUGH

    @staticmethod
    def check_options(o: FieldOptions) -> None:
        o.reject_unknown("passthrough")

    def apply(self, value, record=None, stats=None):
        return value

    def describe(self) -> dict:
        return {}


# ---------------------------------------------------------------- mask

class MaskTransform:
    """Hide part of the value (section 4.2). Counts characters as code points."""

    method = Method.MASK

    def __init__(self, keep_first: int, keep_last: int, keep_domain: bool, mask_char: str, preserve: frozenset,
                 fixed_length: int | None):
        self.keep_first = keep_first
        self.keep_last = keep_last
        self.keep_domain = keep_domain
        self.mask_char = mask_char
        self.preserve = preserve
        self.fixed_length = fixed_length

    @classmethod
    def create(cls, o: FieldOptions) -> "MaskTransform":
        keep_first = o.integer("keep-first", 0, 0, 1000)
        keep_last = o.integer("keep-last", 0, 0, 1000)
        keep = o.string("keep", None)
        keep_domain = False
        if keep is not None:
            if keep != "domain":
                raise ValueError("option 'keep' supports only 'domain'")
            if keep_last > 0:
                raise ValueError("'keep: domain' can't be combined with 'keep-last'")
            keep_domain = True
        ch = o.string("char", "*")
        if len(ch) != 1:
            raise ValueError("option 'char' must be exactly one character")
        preserve = set()
        for p in o.string_list("preserve"):
            if len(p) != 1:
                raise ValueError("each 'preserve' entry must be one character")
            preserve.add(p)
        fixed = None
        if o.has("fixed-length"):
            fixed = o.integer("fixed-length", 0, 1, 1000)
            if preserve:
                raise ValueError("'fixed-length' can't be combined with 'preserve'")
        o.reject_unknown("mask")
        return cls(keep_first, keep_last, keep_domain, ch, frozenset(preserve), fixed)

    def apply(self, value, record=None, stats=None):
        if not value:
            return value
        if self.keep_domain:
            at = value.rfind("@")
            if at < 0:
                return self._mask_part(value, 0, 0)
            return self._mask_part(value[:at], self.keep_first, 0) + value[at:]
        return self._mask_part(value, self.keep_first, self.keep_last)

    def _mask_part(self, cps: str, keep_first: int, keep_last: int) -> str:
        too_short = len(cps) <= keep_first + keep_last
        first = 0 if too_short else keep_first
        last = 0 if too_short else keep_last
        out = [cps[:first]]
        if self.fixed_length is not None:
            out.append(self.mask_char * self.fixed_length)
        else:
            out.extend(c if c in self.preserve else self.mask_char for c in cps[first:len(cps) - last])
        out.append(cps[len(cps) - last:])
        return "".join(out)

    def describe(self) -> dict:
        d = {}
        if self.keep_first > 0:
            d["keep-first"] = self.keep_first
        if self.keep_last > 0:
            d["keep-last"] = self.keep_last
        if self.keep_domain:
            d["keep"] = "domain"
        if self.fixed_length is not None:
            d["fixed-length"] = self.fixed_length
        if self.preserve:
            d["preserve"] = sorted(self.preserve)
        return d


# ---------------------------------------------------------------- generalize

AGE_CAP = 90
_RESTRICTED_ZIP3: frozenset | None = None


def default_restricted_zip3() -> frozenset:
    """The shipped HHS list (``restricted-zip3.txt``, the same file as the Java resource)."""
    global _RESTRICTED_ZIP3
    if _RESTRICTED_ZIP3 is None:
        text = resources.files(__package__).joinpath("restricted-zip3.txt").read_text(encoding="utf-8")
        _RESTRICTED_ZIP3 = frozenset(line.strip() for line in text.splitlines()
                                     if line.strip() and not line.lstrip().startswith("#"))
    return _RESTRICTED_ZIP3


class _To(enum.Enum):
    YEAR = "year"
    BIRTH_YEAR = "birth-year"
    ZIP3 = "zip3"
    AGE_90 = "age-90"
    AGE_BAND = "age-band"

    @classmethod
    def from_label(cls, label: str) -> "_To":
        for t in cls:
            if t.value == label:
                return t
        raise ValueError("option 'to' must be one of year, birth-year, zip3, age-90, age-band")


_ZIP = re.compile(r"([0-9]{5})(?:-?([0-9]{4}))?")
_AGE = re.compile(r"[+-]?\d+")


class GeneralizeTransform:
    """Make the value less precise (section 4.3). ``birth-year`` caps ages over 89."""

    method = Method.GENERALIZE

    def __init__(self, to: _To, input_format, input_format_text, width, over_89_as, as_of, clock, restricted_zip3):
        self.to = to
        self.input_format = input_format
        self.input_format_text = input_format_text
        self.width = width
        self.over_89_as = over_89_as
        self.as_of = as_of
        self._clock = clock
        self._restricted = restricted_zip3

    @classmethod
    def create(cls, o: FieldOptions, clock=None, restricted_zip3=None) -> "GeneralizeTransform":
        """``clock`` returns the run's time (aware or UTC); ``restricted_zip3`` None means the shipped list."""
        to_text = o.string("to", None)
        if to_text is None:
            raise ValueError("option 'to' is required (year, birth-year, zip3, age-90 or age-band)")
        to = _To.from_label(to_text)
        fmt = fmt_text = over89 = as_of = None
        width = 10
        if to in (_To.YEAR, _To.BIRTH_YEAR):
            fmt_text = o.string("input-format", None)
            if fmt_text is not None:
                fmt = JavaDatePattern(fmt_text)
            if to is _To.BIRTH_YEAR:
                over89 = o.string("over-89-as", None)
                as_of = o.string("as-of", None)
        elif to is _To.AGE_BAND:
            width = o.integer("width", 10, 2, 50)
        o.reject_unknown("generalize to: " + to.value)
        return cls(to, fmt, fmt_text, width, over89, as_of, clock or _utc_now,
                   default_restricted_zip3() if restricted_zip3 is None else frozenset(restricted_zip3))

    @property
    def is_birth_year(self) -> bool:
        return self.to is _To.BIRTH_YEAR

    def apply(self, value, record=None, stats=None):
        if not value:
            return value
        stats = stats if stats is not None else FieldStats()
        if self.to is _To.YEAR:
            return str(self._year(value))
        if self.to is _To.BIRTH_YEAR:
            born = self._year(value)
            cutoff = self._as_of_year(record) - AGE_CAP
            if born <= cutoff:
                stats.increment("capped")
                return self.over_89_as if self.over_89_as is not None else "<=" + str(cutoff)
            return str(born)
        if self.to is _To.ZIP3:
            m = _ZIP.fullmatch(_java.trim(value))
            if not m:
                raise DeidError.bad_value("not a 5-digit ZIP or ZIP+4")
            zip3 = m.group(1)[:3]
            if zip3 in self._restricted:
                stats.increment("restricted")
                return "000"
            return zip3
        age = _age(value)
        if age >= AGE_CAP:
            stats.increment("capped")
            return "90+"
        if self.to is _To.AGE_90:
            return str(age)
        low = (age // self.width) * self.width
        high = min(low + self.width - 1, AGE_CAP - 1)
        return str(low) + "-" + str(high)

    def run_year(self) -> int:
        now = self._clock()
        if now.tzinfo is not None:
            now = now.astimezone(_dt.timezone.utc)
        return now.year

    def _as_of_year(self, record) -> int:
        if self.as_of is not None:
            v = record.get(self.as_of) if record else None
            if v:
                try:
                    return self._year(v)
                except DeidError:
                    raise DeidError.bad_value("the as-of field '" + self.as_of + "' is not a date") from None
        return self.run_year()

    def _year(self, value: str) -> int:
        v = _java.trim(value)
        if self.input_format is not None:
            year = self.input_format.year(v)
            if year is None:
                raise DeidError.bad_value("not a date in the input-format")
            return year
        year = iso_year(v)
        if year is None:
            raise DeidError.bad_value("not an ISO date or date-time; set input-format")
        return year

    def describe(self) -> dict:
        d = {"to": self.to.value}
        if self.input_format_text is not None:
            d["input-format"] = self.input_format_text
        if self.to is _To.AGE_BAND:
            d["width"] = self.width
        if self.to is _To.BIRTH_YEAR:
            if self.as_of is not None:
                d["as-of"] = self.as_of
            else:
                d["cutoff_year"] = self.run_year() - AGE_CAP
            if self.over_89_as is not None:
                d["over-89-as"] = self.over_89_as
        return d


def _utc_now() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


def _age(value: str) -> int:
    v = _java.trim(value)
    if not _AGE.fullmatch(v):
        raise DeidError.bad_value("not a whole-number age")
    age = int(v)
    if age < _java.INT_MIN or age > _java.INT_MAX:
        raise DeidError.bad_value("not a whole-number age")
    if age < 0 or age > 150:
        raise DeidError.bad_value("age out of range 0-150")
    return age


# ISO forms, as java.time parses them in GeneralizeTransform's order: LocalDate,
# OffsetDateTime, LocalDateTime, Instant. 'T' and 'Z' are case-insensitive there.
_ISO_DATE = r"(?P<y>-?[0-9]{4}|[+-][0-9]{5,10})-(?P<mo>[0-9]{2})-(?P<d>[0-9]{2})"
_ISO_TIME = r"(?P<h>[0-9]{2}):(?P<mi>[0-9]{2})(?::(?P<s>[0-9]{2})(?:\.(?P<f>[0-9]{0,9}))?)?"
# +HH, +HH:MM, +HH:MM:SS, +HHMM or +HHMMSS: colons all or none, as java.time accepts
_ISO_OFFSET = (r"(?P<z>[Zz])|(?P<oh>[+-][0-9]{2})"
               r"(?::(?P<om>[0-9]{2})(?::(?P<os>[0-9]{2}))?|(?P<om2>[0-9]{2})(?P<os2>[0-9]{2})?)?")
_ISO = re.compile(_ISO_DATE + r"(?:[Tt]" + _ISO_TIME + r"(?:" + _ISO_OFFSET + r")?)?")


def iso_year(v: str) -> int | None:
    """The year of an ISO date or date-time as java.time reads it, or None."""
    m = _ISO.fullmatch(v)
    if not m:
        return None
    year, month, day = int(m.group("y")), int(m.group("mo")), int(m.group("d"))
    if not (1 <= month <= 12) or not (1 <= day <= _month_length(year, month)):
        return None
    if m.group("h") is not None:
        hour, minute = int(m.group("h")), int(m.group("mi"))
        second = int(m.group("s")) if m.group("s") is not None else 0
        if hour > 23 or minute > 59:
            return None
        offset_hours = int(m.group("oh")) if m.group("oh") is not None else 0
        off_min = m.group("om") or m.group("om2")
        off_sec = m.group("os") or m.group("os2")
        if m.group("oh") is not None and (abs(offset_hours) > 18 or int(off_min or 0) > 59
                                          or int(off_sec or 0) > 59):
            return None
        if second > 59:
            # Only Instant.parse accepts a leap second (23:59:60), and it converts to UTC.
            if second != 60 or hour != 23 or minute != 59 or (m.group("z") is None and m.group("oh") is None):
                return None
            off = _dt.timedelta(hours=offset_hours, minutes=_signed(m.group("oh"), off_min))
            utc = _dt.datetime(year, month, day, 23, 59, 59) - off
            return utc.year
    return year


def _signed(oh: str | None, om: str | None) -> int:
    if oh is None:
        return 0
    minutes = int(om or 0)
    return -minutes if oh.startswith("-") else minutes


def _month_length(year: int, month: int) -> int:
    if month == 2:
        leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
        return 29 if leap else 28
    return 30 if month in (4, 6, 9, 11) else 31


class JavaDatePattern:
    """
    A java.time DateTimeFormatter pattern (Locale.ROOT, SMART resolving), read only
    for the year. Supports the letters date columns use: y u M L d D H k K h m s S a
    and literals; any other letter is refused when the profile is checked, so the
    Python job never reads a date differently from the Java job.
    """

    SUPPORTED = set("yuMLdDHkKhmsSa")
    _MONTHS_SHORT = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    def __init__(self, pattern: str):
        self.pattern = pattern
        self._groups: list[tuple[str, int]] = []   # (letter, count) per capture group
        self._regex = re.compile(self._compile(pattern))

    def _compile(self, p: str) -> str:
        out = []
        i = 0
        unsupported = set()
        while i < len(p):
            c = p[i]
            if c == "'":
                j = i + 1
                lit = []
                while True:
                    if j >= len(p):
                        raise ValueError("option 'input-format' is not a valid date pattern")
                    if p[j] == "'":
                        if j + 1 < len(p) and p[j + 1] == "'":
                            lit.append("'")
                            j += 2
                            continue
                        break
                    lit.append(p[j])
                    j += 1
                out.append(re.escape("".join(lit)) if lit else re.escape("'"))
                i = j + 1
                continue
            if ("A" <= c <= "Z") or ("a" <= c <= "z"):
                j = i
                while j < len(p) and p[j] == c:
                    j += 1
                count = j - i
                if c not in self.SUPPORTED:
                    unsupported.add(c)
                else:
                    out.append(self._field(c, count))
                i = j
                continue
            if c in "[]{}#":
                raise ValueError("option 'input-format' is not a valid date pattern")
            out.append(re.escape(c))
            i += 1
        if unsupported:
            raise ValueError("option 'input-format' uses pattern letter(s) " + ", ".join(sorted(unsupported))
                             + " that the Python job doesn't read; use y, u, M, d, H, h, m, s, S, a and literals")
        return "".join(out)

    def _field(self, c: str, count: int) -> str:
        self._groups.append((c, count))
        if c in "yu":
            if count == 2:
                return r"([0-9]{2})"                          # reduced: 2000-2099
            if count < 4:
                return r"([+-]?[0-9]{%d,19})" % count if c == "u" else r"([0-9]{%d,19})" % count
            # 4 or more: EXCEEDS_PAD, so more digits need a sign, which year-of-era (y) can't have
            if c == "y":
                return r"([0-9]{%d})" % count
            return r"(-?[0-9]{%d}|[+-][0-9]{%d,19})" % (count, count + 1)
        if c == "M" and count == 3:
            return "(" + "|".join(self._MONTHS_SHORT) + ")"
        if c == "M" and count >= 4:
            # Locale.ROOT has no full month names Java would match, so nothing parses
            raise ValueError("option 'input-format' uses '" + c * count + "', which the Python job doesn't read")
        if c == "S":
            return r"([0-9]{%d})" % count
        if c == "a":
            if count != 1:
                raise ValueError("option 'input-format' is not a valid date pattern")
            return r"(AM|PM)"
        if count > (3 if c == "D" else 2):
            raise ValueError("option 'input-format' uses '" + c * count + "', which the Python job doesn't read")
        return r"([0-9]{1,19})" if count == 1 else r"([0-9]{%d})" % count

    def year(self, v: str) -> int | None:
        """The year as java.time's SMART resolver gives it, or None when the text doesn't parse."""
        m = self._regex.fullmatch(v)
        if not m:
            return None
        year = month = day = day_of_year = hour24 = None
        minute = second = 0
        for (c, count), text in zip(self._groups, m.groups()):
            if c in "yu":
                n = int(text)
                if count == 2:
                    n = 2000 + n
                elif c == "y" and n < 1:
                    return None
                year = n
            elif c in "ML":
                month = self._MONTHS_SHORT.index(text) + 1 if count == 3 else int(text)
                if not 1 <= month <= 12:
                    return None
            elif c == "d":
                day = int(text)
                if not 1 <= day <= 31:
                    return None
            elif c == "D":
                day_of_year = int(text)
                if not 1 <= day_of_year <= 366:
                    return None
            elif c == "H":
                hour24 = int(text)
                if not 0 <= hour24 <= 24:
                    return None
            elif c == "k":
                if not 0 <= int(text) <= 24:      # SMART: 0 accepted, 24 is midnight of the same day
                    return None
            elif c == "K":
                if not 0 <= int(text) <= 11:
                    return None
            elif c == "h":
                if not 0 <= int(text) <= 12:      # SMART: 0 accepted
                    return None
            elif c == "m":
                minute = int(text)
                if minute > 59:
                    return None
            elif c == "s":
                second = int(text)
                if second > 59:
                    return None
        if hour24 == 24:
            # SMART reads 24:00 as midnight ending the day: valid only on the hour, and
            # it moves the date one day on, which changes the year on the last day.
            if minute or second:
                return None
            if year is not None:
                year_length = 366 if _month_length(year, 2) == 29 else 365
                if (month == 12 and day == 31) or day_of_year == year_length:
                    year += 1
        return year


# ---------------------------------------------------------------- tokenize

TOKEN_BYTES = 16


class TokenizeTransform:
    """A consistent code computed from the value (section 4.4), pinned by deid-test-vectors.json."""

    method = Method.TOKENIZE

    def __init__(self, normalize: Normalize, prefix: str, key: bytes, domain: str):
        self.normalize = normalize
        self.prefix = prefix
        self._key = key
        self.domain = domain
        self._domain_prefix = domain_prefix(domain)

    @classmethod
    def create(cls, o: FieldOptions, key: bytes, domain: str) -> "TokenizeTransform":
        normalize = Normalize.from_label(o.string("normalize", "trim"))
        prefix = o.string("prefix", "tk_")
        o.reject_unknown("tokenize")
        return cls(normalize, prefix, key, domain)

    def apply(self, value, record=None, stats=None):
        if not value:
            return value
        return token(self._key, self._domain_prefix, self.normalize.apply(value), self.prefix)

    def describe(self) -> dict:
        return {"domain": self.domain, "normalize": self.normalize.label}


def token(key: bytes, prefix_bytes: bytes, normalized: str, prefix: str = "tk_") -> str:
    mac = hmac_sha256(key, prefix_bytes, normalized.encode("utf-8"))
    return prefix + base64.urlsafe_b64encode(mac[:TOKEN_BYTES]).rstrip(b"=").decode("ascii")


# ---------------------------------------------------------------- registry

def token_domain(profile: DeidProfile, f: Field) -> str:
    domain = f.domain if f.domain is not None else profile.token_domain
    if domain is None or not _java.strip(domain):
        raise ValueError("tokenize needs a domain (field domain or profile token-domain)")
    return domain


def check_options(profile: DeidProfile, f: Field) -> None:
    """Checks one field's options without keys, a store or a clock (for the validator)."""
    from .surrogate import SurrogateTransform  # avoids an import cycle
    o = FieldOptions(f.options)
    if f.method is Method.DROP:
        DropTransform.check_options(o)
    elif f.method is Method.PASSTHROUGH:
        PassthroughTransform.check_options(o)
    elif f.method is Method.MASK:
        MaskTransform.create(o)
    elif f.method is Method.GENERALIZE:
        GeneralizeTransform.create(o)
    elif f.method is Method.TOKENIZE:
        TokenizeTransform.create(o, bytes(32), token_domain(profile, f))
    elif f.method is Method.SURROGATE:
        SurrogateTransform.create(o, f.domain, "check", (bytes(32), bytes(32)), None)


def generalize_of(f: Field) -> GeneralizeTransform | None:
    """The field's generalize transform, or None, so the validator can see ``to`` and ``as-of``."""
    if f.method is not Method.GENERALIZE:
        return None
    return GeneralizeTransform.create(FieldOptions(f.options))


def create(profile: DeidProfile, f: Field, *, keys: KeyProvider | None = None, clock=None,
           restricted_zip3=None, surrogate_key_edek_id=None):
    """
    The configured transform for one field. A surrogate field gets a
    ``SurrogateTransform`` with its keys; the surrogate table is used by the job
    (phase 5), not here.
    """
    from .surrogate import SurrogateTransform
    o = FieldOptions(f.options)
    if f.method is Method.DROP:
        DropTransform.check_options(o)
        return DropTransform()
    if f.method is Method.PASSTHROUGH:
        PassthroughTransform.check_options(o)
        return PassthroughTransform()
    if f.method is Method.MASK:
        return MaskTransform.create(o)
    if f.method is Method.GENERALIZE:
        return GeneralizeTransform.create(o, clock, restricted_zip3)
    if keys is None:
        raise RuntimeError("this profile needs keys from hsm-core, but the run has no KeyProvider")
    if f.method is Method.TOKENIZE:
        if profile.token_key_edek_id is None:
            raise RuntimeError("tokenize needs token-key-edek-id in the profile")
        return TokenizeTransform.create(o, keys.key(profile.token_key_edek_id, TOKENIZE_LABEL),
                                        token_domain(profile, f))
    if surrogate_key_edek_id is None:
        raise RuntimeError("surrogate needs the surrogate-table key (surrogate-key-edek-id)")
    if profile.surrogate_scope is None:
        raise RuntimeError("surrogate needs surrogate-scope in the profile")
    return SurrogateTransform.create(o, f.domain, profile.surrogate_scope,
                                     keys.keys(surrogate_key_edek_id, *SurrogateTransform.key_labels()),
                                     surrogate_key_edek_id)
