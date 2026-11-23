"""
The rules a profile must pass before any record is read (java/docs/DEIDENTIFICATION.md
sections 4.7, 6.2 and 6.3), as com.hsm.client.deid.ProfileValidator: the same rules,
the same messages, in the same order.

The validator can't spot an identifier that isn't tagged with ``phi``: the dry run
and the manifest list passthrough and dropped fields for the reviewer.
"""

from __future__ import annotations

from collections.abc import Collection

from . import _java, methods
from .errors import DeidValidationError
from .profile import DeidProfile, HipaaBasis, Method, PhiGroup


def validate(profile: DeidProfile, source_columns: Collection[str] | None) -> None:
    """Raises DeidValidationError listing every problem; ``source_columns`` None skips the source checks."""
    found = problems(profile, source_columns)
    if found:
        raise DeidValidationError(found)


def problems(profile: DeidProfile, source_columns: Collection[str] | None) -> list[str]:
    """Every problem found; empty when the profile can run."""
    out: list[str] = []
    names: set[str] = set()
    outputs: set[str] = set()
    source = None if source_columns is None else set(source_columns)

    if profile.basis is HipaaBasis.EXPERT_DETERMINATION and not _java.strip(profile.expert_report or ""):
        out.append("expert-determination needs expert-report: <reference to the expert's report>")

    for f in profile.fields:
        where = "field '" + f.name + "'"
        if f.name in names:
            out.append(where + " is listed more than once")
        names.add(f.name)
        if source is not None and f.name not in source:
            out.append(where + " is not in the source")

        try:
            methods.check_options(profile, f)
        except ValueError as e:
            out.append(where + ": " + str(e))

        forbidden = profile.forbidden_fields.search(f.name) is not None
        if forbidden and f.method is not Method.DROP:
            out.append(where + " is a never-export field (PIN, CVV, activation or security code): only drop is allowed")

        if f.decrypt and f.method is Method.DROP:
            out.append(where + ": decrypt: true with drop has nothing to decrypt for; remove decrypt")

        if f.method is Method.TOKENIZE and profile.token_key_edek_id is None:
            out.append(where + ": tokenize needs token-key-edek-id in the profile")
        if f.method is Method.SURROGATE and profile.surrogate_scope is None:
            out.append(where + ": surrogate needs surrogate-scope in the profile")

        generalize = _safe_generalize(f)
        if generalize is not None and generalize.as_of is not None and source is not None \
                and generalize.as_of not in source:
            out.append(where + ": as-of field '" + generalize.as_of + "' is not in the source")

        if profile.basis is HipaaBasis.SAFE_HARBOR:
            _safe_harbor(f, generalize, where, out)

        if f.method is not Method.DROP:
            name = profile.output_name(f)
            if name in outputs:
                out.append(where + ": output column '" + name + "' is used more than once")
            outputs.add(name)
            if profile.forbidden_fields.search(name) is not None:
                out.append(where + ": output column '" + name + "' looks like a never-export field; rename it")

    for key, value in profile.output.rename.items():
        listed = any(f.name == key and f.method is not Method.DROP for f in profile.fields)
        if not listed:
            out.append("output.rename: '" + key + "' is not a listed, exported field")
        if value is None or not _java.strip(value):
            out.append("output.rename: '" + key + "' has an empty output name")
    return out


def _safe_harbor(f, generalize, where: str, out: list[str]) -> None:
    """Section 6.2: which methods a Safe Harbor identifier may use."""
    if f.phi is None or f.method is Method.DROP:
        return
    group = f.phi.group
    category = "phi: " + f.phi.label
    if group is PhiGroup.DIRECT:
        out.append(where + " (" + category + "): safe-harbor allows only drop")
    elif group is PhiGroup.UNIQUE_ID:
        if f.method is not Method.SURROGATE:
            out.append(where + " (" + category + "): safe-harbor allows only drop or surrogate"
                       + (" (a token is derived from the value, which §164.514(c) forbids)"
                          if f.method is Method.TOKENIZE else ""))
    elif group is PhiGroup.BIRTH_DATE:
        if generalize is None or not generalize.is_birth_year:
            out.append(where + " (" + category + "): safe-harbor allows only drop or generalize to: birth-year"
                       + " (to: year would show ages over 89)")
        elif generalize.as_of is not None:
            out.append(where + " (" + category + "): safe-harbor doesn't allow as-of"
                       + " (an age measured at an older date understates today's age)")
    elif group is PhiGroup.DATE:
        _require_generalize(generalize, ("year",), where, category, out)
    elif group is PhiGroup.ZIP:
        _require_generalize(generalize, ("zip3",), where, category, out)
    elif group is PhiGroup.AGE:
        _require_generalize(generalize, ("age-90", "age-band"), where, category, out)


def _require_generalize(generalize, allowed: tuple, where: str, category: str, out: list[str]) -> None:
    to = None if generalize is None else generalize.to.value
    if to not in allowed:
        out.append(where + " (" + category + "): safe-harbor allows only drop or generalize to: " + " / ".join(allowed))


def _safe_generalize(f):
    try:
        return methods.generalize_of(f)
    except ValueError:
        return None  # already reported by check_options
