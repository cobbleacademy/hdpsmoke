"""
A de-identification profile (java/docs/DEIDENTIFICATION.md section 6), as
com.hsm.client.deid.DeidProfile, PhiCategory, HipaaBasis and ColumnTransform.Method.

``DeidProfile.from_map`` reads a profile document already parsed into dicts and
lists and checks its structure only; ``validator.problems`` checks the rules.
``parse_yaml`` reads the YAML bytes the way the Java job does (safe loading only).
Every message matches the Java one word for word.
"""

from __future__ import annotations

import enum
import re
import uuid
from dataclasses import dataclass, field as dc_field

import yaml

from . import _java
from .errors import DeidValidationError

DEFAULT_FORBIDDEN_FIELDS = (
    r"(?i)(^|[_.\-\]])(pin|pin_?code|cvv2?|cvc2?|activation(_?code)?|security_?code|passcode)($|[_.\-\[])"
)


class Method(enum.Enum):
    DROP = "drop"
    MASK = "mask"
    GENERALIZE = "generalize"
    TOKENIZE = "tokenize"
    SURROGATE = "surrogate"
    PASSTHROUGH = "passthrough"

    @property
    def label(self) -> str:
        return self.value

    @property
    def id(self) -> str:
        """As recorded in the manifest, e.g. ``mask@v1``."""
        return self.value + "@v1"

    @classmethod
    def from_label(cls, label: str) -> "Method":
        for m in cls:
            if m.value == label:
                return m
        raise ValueError("unknown method '" + label
                         + "' (expected drop, mask, generalize, tokenize, surrogate or passthrough)")


class PhiGroup(enum.Enum):
    DIRECT = "direct"          # drop only
    UNIQUE_ID = "unique-id"    # drop, or a surrogate
    BIRTH_DATE = "birth-date"  # drop, or birth-year
    DATE = "date"              # drop, or year
    ZIP = "zip"                # drop, or zip3
    AGE = "age"                # drop, or age-90 / age-band


class PhiCategory(enum.Enum):
    NAME = ("name", PhiGroup.DIRECT)
    ADDRESS = ("address", PhiGroup.DIRECT)
    EMAIL = ("email", PhiGroup.DIRECT)
    PHONE = ("phone", PhiGroup.DIRECT)
    FAX = ("fax", PhiGroup.DIRECT)
    SSN = ("ssn", PhiGroup.DIRECT)
    URL = ("url", PhiGroup.DIRECT)
    IP = ("ip", PhiGroup.DIRECT)
    DEVICE_ID = ("device-id", PhiGroup.DIRECT)
    LICENSE = ("license", PhiGroup.DIRECT)
    VEHICLE_ID = ("vehicle-id", PhiGroup.DIRECT)
    BIOMETRIC = ("biometric", PhiGroup.DIRECT)
    PHOTO = ("photo", PhiGroup.DIRECT)
    FREE_TEXT = ("free-text", PhiGroup.DIRECT)

    MEMBER_ID = ("member-id", PhiGroup.UNIQUE_ID)
    MRN = ("mrn", PhiGroup.UNIQUE_ID)
    ACCOUNT = ("account", PhiGroup.UNIQUE_ID)
    CARD_NUMBER = ("card-number", PhiGroup.UNIQUE_ID)
    PLAN_ID = ("plan-id", PhiGroup.UNIQUE_ID)
    UNIQUE_ID = ("unique-id", PhiGroup.UNIQUE_ID)

    BIRTH_DATE = ("birth-date", PhiGroup.BIRTH_DATE)
    DATE = ("date", PhiGroup.DATE)
    ZIP = ("zip", PhiGroup.ZIP)
    AGE = ("age", PhiGroup.AGE)

    @property
    def label(self) -> str:
        return self.value[0]

    @property
    def group(self) -> PhiGroup:
        return self.value[1]

    @classmethod
    def from_label(cls, label: str) -> "PhiCategory":
        for c in cls:
            if c.label == label:
                return c
        raise ValueError("unknown phi category '" + label + "'")


class HipaaBasis(enum.Enum):
    MINIMUM_NECESSARY = "minimum-necessary"
    SAFE_HARBOR = "safe-harbor"
    EXPERT_DETERMINATION = "expert-determination"

    @property
    def label(self) -> str:
        return self.value

    @classmethod
    def from_label(cls, label: str) -> "HipaaBasis":
        for b in cls:
            if b.value == label:
                return b
        raise ValueError("unknown hipaa-basis '" + label
                         + "' (expected minimum-necessary, safe-harbor or expert-determination)")


@dataclass(frozen=True)
class Field:
    """One listed field: a source column or JSON path, its method and options."""

    name: str
    phi: PhiCategory | None
    method: Method
    decrypt: bool
    domain: str | None
    options: dict = dc_field(default_factory=dict)


@dataclass(frozen=True)
class Output:
    file: str | None
    rename: dict
    delimiter: str = ","
    crlf: bool = False
    formula_guard: bool = True


_TOP_LEVEL_KEYS = frozenset({
    "id", "version", "vendor", "hipaa-basis", "expert-report", "token-key-edek-id", "token-domain",
    "token-epoch", "surrogate-scope", "forbidden-fields", "fields", "output"})
_FIELD_KEYS = frozenset({"name", "phi", "method", "decrypt", "domain"})
_OUTPUT_KEYS = frozenset({"file", "rename", "delimiter", "line-ending", "formula-guard"})


def compile_forbidden(pattern: str) -> re.Pattern:
    """Java's Pattern defaults: ASCII-only case folding and character classes."""
    return re.compile(pattern, re.ASCII)


@dataclass(frozen=True)
class DeidProfile:
    id: str
    version: int
    vendor: str | None
    basis: HipaaBasis
    expert_report: str | None
    token_key_edek_id: uuid.UUID | None
    token_domain: str | None
    token_epoch: int
    surrogate_scope: str | None
    forbidden_fields: re.Pattern
    fields: tuple
    output: Output

    def output_name(self, f: Field) -> str:
        """Renamed, or the source name with a JSON path's ``[]`` left out."""
        return self.output.rename.get(f.name, f.name.replace("[]", ""))

    @staticmethod
    def from_map(doc: dict) -> "DeidProfile":
        """Reads a parsed profile document; raises DeidValidationError listing every structural problem."""
        if not isinstance(doc, dict):
            raise DeidValidationError(["the profile is not a YAML mapping"])
        problems: list[str] = []
        _unknown_keys(doc.keys(), _TOP_LEVEL_KEYS, "profile", problems)

        pid = _required_string(doc, "id", problems)
        version = _integer(doc.get("version"), "version", problems)
        vendor = _optional_string(doc.get("vendor"))
        basis = None
        basis_label = _required_string(doc, "hipaa-basis", problems)
        if basis_label is not None:
            try:
                basis = HipaaBasis.from_label(basis_label)
            except ValueError as e:
                problems.append(str(e))
        token_key = None
        token_key_text = _optional_string(doc.get("token-key-edek-id"))
        if token_key_text is not None:
            try:
                token_key = _uuid(token_key_text)
            except ValueError:
                problems.append("token-key-edek-id must be a UUID (an edek_id from --deid.init-key)")
        token_epoch = 1 if doc.get("token-epoch") is None else _integer(doc.get("token-epoch"), "token-epoch", problems)

        forbidden = None
        forbidden_text = _optional_string(doc.get("forbidden-fields"))
        try:
            forbidden = compile_forbidden(forbidden_text if forbidden_text is not None else DEFAULT_FORBIDDEN_FIELDS)
        except re.error:
            problems.append("forbidden-fields is not a valid regular expression")

        fields = _parse_fields(doc.get("fields"), problems)
        output = _parse_output(doc.get("output"), problems)

        if problems:
            raise DeidValidationError(problems)
        return DeidProfile(pid, version, vendor, basis, _optional_string(doc.get("expert-report")), token_key,
                           _optional_string(doc.get("token-domain")), token_epoch,
                           _optional_string(doc.get("surrogate-scope")), forbidden, tuple(fields), output)


def parse_yaml(data: bytes | str) -> DeidProfile:
    """Parses profile YAML (safe loading: no custom tags) and reads it with ``from_map``."""
    try:
        doc = yaml.safe_load(data)
    except yaml.YAMLError as e:
        mark = getattr(e, "problem_mark", None)
        where = " at line " + str(mark.line + 1) + ", column " + str(mark.column + 1) if mark else ""
        raise DeidValidationError(["the profile is not valid YAML" + where]) from None
    return DeidProfile.from_map(doc)


def _uuid(text: str) -> uuid.UUID:
    # java.util.UUID.fromString takes the 8-4-4-4-12 form; Python's UUID() also takes
    # 32 hex digits and braces, so check the form first.
    if not re.fullmatch(r"[0-9a-fA-F]{1,8}-[0-9a-fA-F]{1,4}-[0-9a-fA-F]{1,4}-[0-9a-fA-F]{1,4}-[0-9a-fA-F]{1,12}", text):
        raise ValueError("not a UUID")
    parts = text.split("-")
    widths = (8, 4, 4, 4, 12)
    return uuid.UUID("-".join(p.rjust(w, "0") for p, w in zip(parts, widths)))


def _parse_fields(raw, problems: list[str]) -> list[Field]:
    fields: list[Field] = []
    if not isinstance(raw, list) or not raw:
        problems.append("fields must be a non-empty list")
        return fields
    for i, entry in enumerate(raw):
        if not isinstance(entry, dict):
            problems.append("fields[" + str(i) + "] must be a map with name and method")
            continue
        name = _optional_string(entry.get("name"))
        where = "field '" + name + "'" if name is not None else "fields[" + str(i) + "]"
        if name is None:
            problems.append(where + ": name is required")
            continue
        method = None
        method_label = _optional_string(entry.get("method"))
        if method_label is None:
            problems.append(where + ": method is required")
        else:
            try:
                method = Method.from_label(method_label)
            except ValueError as e:
                problems.append(where + ": " + str(e))
        phi = None
        phi_label = _optional_string(entry.get("phi"))
        if phi_label is not None:
            try:
                phi = PhiCategory.from_label(phi_label)
            except ValueError as e:
                problems.append(where + ": " + str(e))
        decrypt = False
        decrypt_raw = entry.get("decrypt")
        if isinstance(decrypt_raw, bool):
            decrypt = decrypt_raw
        elif decrypt_raw is not None:
            problems.append(where + ": decrypt must be true or false")
        options = {}
        for k, v in entry.items():
            key = _java.to_string(k)
            if key not in _FIELD_KEYS:
                options[key] = v
        if method is not None:
            fields.append(Field(name, phi, method, decrypt, _optional_string(entry.get("domain")), options))
    return fields


def _parse_output(raw, problems: list[str]) -> Output | None:
    if not isinstance(raw, dict):
        problems.append("output must be a map with at least file")
        return None
    _unknown_keys(raw.keys(), _OUTPUT_KEYS, "output", problems)
    file = _optional_string(raw.get("file"))
    if file is None:
        problems.append("output.file is required")
    rename = {}
    rename_raw = raw.get("rename")
    if isinstance(rename_raw, dict):
        for k, v in rename_raw.items():
            rename[_java.to_string(k)] = _java.to_string(v)
    elif rename_raw is not None:
        problems.append("output.rename must be a map of source name to output name")
    delimiter = ","
    delimiter_text = _optional_string(raw.get("delimiter"))
    if delimiter_text is not None:
        if len(delimiter_text.encode("utf-16-le")) != 2 or delimiter_text in ('"', "\n", "\r"):
            problems.append("output.delimiter must be one character other than a quote or line break")
        else:
            delimiter = delimiter_text
    crlf = False
    line_ending = _optional_string(raw.get("line-ending"))
    if line_ending is not None:
        if line_ending == "lf":
            crlf = False
        elif line_ending == "crlf":
            crlf = True
        else:
            problems.append("output.line-ending must be lf or crlf")
    formula_guard = True
    guard = raw.get("formula-guard")
    if isinstance(guard, bool):
        formula_guard = guard
    elif guard is not None:
        problems.append("output.formula-guard must be true or false")
    return Output(file, rename, delimiter, crlf, formula_guard)


def _unknown_keys(keys, allowed, where: str, problems: list[str]) -> None:
    unknown = sorted({_java.to_string(k) for k in keys} - allowed)
    if unknown:
        problems.append("unknown key(s) in " + where + ": " + ", ".join(unknown))


def _required_string(doc: dict, key: str, problems: list[str]) -> str | None:
    v = _optional_string(doc.get(key))
    if v is None:
        problems.append(key + " is required")
    return v


def _optional_string(v) -> str | None:
    if v is None:
        return None
    s = _java.trim(_java.to_string(v))
    return s or None


def _integer(v, key: str, problems: list[str]) -> int:
    if isinstance(v, int) and not isinstance(v, bool) and _java.INT_MIN <= v <= _java.INT_MAX:
        return v
    if v is not None:
        try:
            return _java.parse_int(_java.trim(_java.to_string(v)))
        except ValueError:
            pass
    problems.append(key + " is required and must be a whole number")
    return 0
