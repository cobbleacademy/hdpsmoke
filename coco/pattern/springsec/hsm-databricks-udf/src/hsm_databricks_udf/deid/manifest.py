"""
``<file>.manifest.json`` (java/docs/DEIDENTIFICATION.md section 10.2), as the Java job's
DeidManifest builds it: the same keys in the same order, and the same layout as
Jackson's default pretty printer, so manifests from both jobs read and diff alike.
Counts and settings only: never values, tokens or surrogates.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass

from .profile import DeidProfile, Method


@dataclass
class Run:
    run_id: str
    profile_sha256: str
    triggered_by: str | None
    started_at: dt.datetime
    completed_at: dt.datetime | None
    surrogate_key_edek_id: str | None
    source_columns: list
    rows: int
    rows_skipped: int
    output_file: str
    output_sha256: str | None
    dry_run: bool


def instant(t: dt.datetime | None) -> str | None:
    """java.time.Instant.toString: UTC, seconds always, the fraction in groups of 3 digits."""
    if t is None:
        return None
    t = t.astimezone(dt.timezone.utc)
    out = t.strftime("%Y-%m-%dT%H:%M:%S")
    if t.microsecond:
        out += "." + (str(t.microsecond // 1000).rjust(3, "0") if t.microsecond % 1000 == 0
                      else str(t.microsecond).rjust(6, "0"))
    return out + "Z"


def build(profile: DeidProfile, reports: list[dict], run: Run, *, decrypted_edek_ids: int,
          surrogate_counts: dict | None = None) -> dict:
    """``reports``: per output field (pipeline.merge_counters), with surrogate counters filled in."""
    m: dict = {"run_id": run.run_id}
    if run.dry_run:
        m["dry_run"] = True
    p = {"id": profile.id, "version": profile.version, "sha256": run.profile_sha256, "vendor": profile.vendor,
         "hipaa_basis": profile.basis.label}
    if profile.expert_report is not None:
        p["expert_report"] = profile.expert_report
    m["profile"] = p
    m["triggered_by"] = run.triggered_by
    m["started_at"] = instant(run.started_at)
    m["completed_at"] = instant(run.completed_at)

    tokenize = any(f.method is Method.TOKENIZE for f in profile.fields)
    surrogate = any(f.method is Method.SURROGATE for f in profile.fields)
    m["keys"] = {"token": {"edek_id": str(profile.token_key_edek_id), "epoch": profile.token_epoch} if tokenize else None,
                 "surrogate": {"edek_id": str(run.surrogate_key_edek_id)} if surrogate else None}

    fields, decrypted = [], []
    new_total = existing_total = 0
    for r in reports:
        f = {"source": r["source"], "output": r["output"]}
        if r["phi"] is not None:
            f["phi"] = r["phi"]
        f["method"] = r["method"]
        for k, v in r["settings"].items():
            f.setdefault(k, v)
        if r["decrypted"]:
            f["decrypted"] = True
            decrypted.append(r["source"])
        f["values"] = r["values"]
        f["empty"] = r["empty"]
        for k, v in r["counters"].items():
            if k != "decrypted":
                f.setdefault(k, v)
        if r["method"].startswith(Method.SURROGATE.label + "@"):
            new_total += r["counters"].get("new", 0)
            existing_total += r["counters"].get("existing", 0)
        fields.append(f)
    m["fields"] = fields
    listed = {f.name for f in profile.fields}
    m["dropped_by_default"] = [c for c in run.source_columns if c not in listed]
    if surrogate:
        m["surrogates"] = {"new": new_total, "existing": existing_total}
    if decrypted:
        m["decrypt"] = {"fields": decrypted, "distinct_edek_ids": decrypted_edek_ids}
    m["output"] = {"file": run.output_file, "rows_in": run.rows + run.rows_skipped, "rows_out": run.rows,
                   "rows_skipped": run.rows_skipped, "sha256": run.output_sha256}
    return m


def to_json(value, indent: int = 0) -> str:
    """Jackson's DefaultPrettyPrinter layout: ``"key" : value``, two-space indent, ``[ a, b ]`` arrays."""
    pad = "  " * indent
    if isinstance(value, dict):
        if not value:
            return "{ }"
        items = [pad + "  " + json.dumps(str(k), ensure_ascii=False) + " : " + to_json(v, indent + 1)
                 for k, v in value.items()]
        return "{\n" + ",\n".join(items) + "\n" + pad + "}"
    if isinstance(value, (list, tuple)):
        if not value:
            return "[ ]"
        return "[ " + ", ".join(to_json(v, indent) for v in value) + " ]"
    return json.dumps(value, ensure_ascii=False)
