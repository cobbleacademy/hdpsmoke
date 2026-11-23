"""
The few Java behaviours the Java implementation (com.hsm.client.deid) relies on,
reproduced exactly, so the Python port gives the same output for the same input.

Python's own versions differ in small ways that would change a token or a mask:
str.strip() removes more characters than Java's String.strip() (e.g. the
no-break space), int() accepts "1_0" and surrounding whitespace, and str(True)
is "True" where Java's String.valueOf gives "true".
"""

from __future__ import annotations

import math
import re
from decimal import Decimal

INT_MIN = -(2 ** 31)
INT_MAX = 2 ** 31 - 1

# Character.isWhitespace: Unicode space separators except the no-break ones
# (U+00A0, U+2007, U+202F), the line and paragraph separators, and
# U+0009-U+000D, U+001C-U+001F.
_JAVA_WHITESPACE = frozenset(
    "\t\n\u000b\f\r\u001c\u001d\u001e\u001f"
    "            "
    " 　"
    "  "
)

_INT = re.compile(r"[+-]?\d+")


def is_whitespace(ch: str) -> bool:
    return ch in _JAVA_WHITESPACE


def strip(s: str) -> str:
    """Java String.strip(): leading and trailing Character.isWhitespace removed."""
    start, end = 0, len(s)
    while start < end and s[start] in _JAVA_WHITESPACE:
        start += 1
    while end > start and s[end - 1] in _JAVA_WHITESPACE:
        end -= 1
    return s[start:end]


def trim(s: str) -> str:
    """Java String.trim(): leading and trailing characters <= U+0020 removed."""
    start, end = 0, len(s)
    while start < end and s[start] <= " ":
        start += 1
    while end > start and s[end - 1] <= " ":
        end -= 1
    return s[start:end]


def to_string(v) -> str:
    """Java String.valueOf for the values a YAML parser produces."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        return double_to_string(v)
    return str(v)


def double_to_string(d: float) -> str:
    """Java Double.toString: shortest round-trip digits; plain in [1e-3, 1e7), else d.dddE<n>."""
    if math.isnan(d):
        return "NaN"
    if math.isinf(d):
        return "Infinity" if d > 0 else "-Infinity"
    if d == 0:
        return "-0.0" if math.copysign(1.0, d) < 0 else "0.0"
    sign, digits, exponent = Decimal(repr(d)).as_tuple()
    text = "".join(map(str, digits)).rstrip("0") or "0"
    point = len(digits) + exponent            # position of the decimal point in the digit string
    neg = "-" if sign else ""
    if 1e-3 <= abs(d) < 1e7:
        if point <= 0:
            return neg + "0." + "0" * (-point) + text
        if point >= len(text):
            return neg + text + "0" * (point - len(text)) + ".0"
        return neg + text[:point] + "." + text[point:]
    return neg + text[0] + "." + (text[1:] or "0") + "E" + str(point - 1)


def parse_int(s: str) -> int:
    """Java Integer.parseInt: an optional sign and digits, nothing else, in int range."""
    if not _INT.fullmatch(s):
        raise ValueError("not an int")
    n = int(s)
    if n < INT_MIN or n > INT_MAX:
        raise ValueError("out of int range")
    return n


def code_point_count(s: str) -> int:
    """Python strings are code points, as Java's codePointCount counts."""
    return len(s)
