"""
One batch of rows through a profile (java/docs/DEIDENTIFICATION.md section 17.3,
steps 1-3), as com.hsm.client.deid.DeidPipeline and FieldDecryptor, in plain Python:
the Spark functions in ``spark_methods.py`` call it per Arrow batch, and the dry run
calls it on the driver.

A batch is a list of records (``dict`` of source column to string or None). For each
exported field the result holds:

- the output value, for every method but surrogate;
- for a surrogate field, the ``lookup_hash`` (hex) and ``value_enc`` of each non-empty
  value instead: surrogates are resolved against the table afterwards, once per
  distinct value (section 17.4). An empty value stays as it is.

Counts go into ``BatchStats`` (plain dicts, so they can travel as JSON and be added
up across batches). Messages name the field and, when the source has a key column,
that row's key; never a value.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field as dc_field
from typing import Callable, Protocol

from . import _java, methods
from .errors import DeidError
from .keys import KeyProvider
from .profile import DeidProfile, Field, Method
from .surrogate import SurrogateTransform

_TOKEN_PREFIX = "v1."


class Hsm(Protocol):
    """What the job needs from hsm-core. ``HsmCoreClient`` (in ``job.py``) is the real one."""

    def unwrap_dek(self, edek_id: uuid.UUID) -> bytes: ...

    def decrypt_to_string(self, token: str) -> str: ...


def needs_hsm(profile: DeidProfile) -> bool:
    return any(f.method is not Method.DROP and (f.decrypt or f.method in (Method.TOKENIZE, Method.SURROGATE))
               for f in profile.fields)


def needs_keys(profile: DeidProfile) -> bool:
    return any(f.method in (Method.TOKENIZE, Method.SURROGATE) for f in profile.fields)


def needs_surrogate_table(profile: DeidProfile) -> bool:
    return any(f.method is Method.SURROGATE for f in profile.fields)


def exported(profile: DeidProfile) -> list[Field]:
    out = [f for f in profile.fields if f.method is not Method.DROP]
    if not out:
        raise RuntimeError("the profile exports no fields: every listed field is drop")
    return out


def columns_to_read(profile: DeidProfile) -> list[str]:
    """Exported fields and the fields their options read (``as-of``); nothing else is read."""
    cols: list[str] = []
    for f in profile.fields:
        if f.method is Method.DROP:
            continue
        if f.name not in cols:
            cols.append(f.name)
        as_of = f.options.get("as-of") if f.method is Method.GENERALIZE else None
        if as_of is not None and _java.to_string(as_of) not in cols:
            cols.append(_java.to_string(as_of))
    return cols


def looks_encrypted(value: str | None) -> bool:
    """Does the value look like HSM ciphertext (a ``v1.`` token that unpacks)?"""
    from .. import dek_manager
    if value is None:
        return False
    v = _java.strip(value)
    if not v.startswith(_TOKEN_PREFIX):
        return False
    try:
        dek_manager.unpack_token(v)
        return True
    except ValueError:
        return False


def unmarked_ciphertext(profile: DeidProfile, sample: list[dict]) -> list[str]:
    """Exported fields not marked ``decrypt: true`` whose sample values are HSM ciphertext."""
    out = []
    for f in exported(profile):
        if not f.decrypt and any(looks_encrypted(r.get(f.name)) for r in sample):
            out.append(f.name)
    return out


@dataclass
class BatchStats:
    """Counts for one batch (or many, added up). Never values."""

    rows: int = 0
    values: dict = dc_field(default_factory=dict)     # output index -> count
    empty: dict = dc_field(default_factory=dict)      # output index -> count
    counters: dict = dc_field(default_factory=dict)   # output index -> {counter: count}
    edek_ids: set = dc_field(default_factory=set)     # distinct edek_ids decrypted

    def add(self, other: "BatchStats") -> None:
        self.rows += other.rows
        for i, n in other.values.items():
            self.values[i] = self.values.get(i, 0) + n
        for i, n in other.empty.items():
            self.empty[i] = self.empty.get(i, 0) + n
        for i, c in other.counters.items():
            mine = self.counters.setdefault(i, {})
            for k, n in c.items():
                mine[k] = mine.get(k, 0) + n
        self.edek_ids |= other.edek_ids

    def to_json_dict(self) -> dict:
        return {"rows": self.rows, "values": {str(k): v for k, v in self.values.items()},
                "empty": {str(k): v for k, v in self.empty.items()},
                "counters": {str(k): v for k, v in self.counters.items()}, "edek_ids": sorted(self.edek_ids)}

    @classmethod
    def from_json_dict(cls, d: dict) -> "BatchStats":
        return cls(d["rows"], {int(k): v for k, v in d["values"].items()}, {int(k): v for k, v in d["empty"].items()},
                   {int(k): dict(v) for k, v in d["counters"].items()}, set(d["edek_ids"]))


@dataclass
class BatchResult:
    """Per output field, in header order: values (non-surrogate) or hashes and value_enc (surrogate)."""

    values: list           # list per output field: output strings; for surrogate fields the source value if empty
    hashes: list           # list per output field: hex lookup hash per row, or None (non-surrogate or empty)
    value_enc: list        # list per output field: value_enc per row, or None
    stats: BatchStats


class BatchProcessor:
    """
    The profile's transforms, ready to run on batches. Build one per worker process
    (or partition): ``hsm`` unwraps each key once, cached by ``KeyProvider``.
    """

    def __init__(self, profile: DeidProfile, *, hsm: Hsm | None, clock: Callable, surrogate_key_edek_id=None,
                 key_column: str | None = None, restricted_zip3=None):
        self.profile = profile
        self.key_column = key_column
        self._hsm = hsm
        self.fields = exported(profile)
        self.header = [profile.output_name(f) for f in self.fields]
        keys = KeyProvider(hsm.unwrap_dek) if needs_keys(profile) and hsm is not None else None
        self._keys = keys
        self.transforms = []
        for f in self.fields:
            if f.decrypt and hsm is None:
                raise RuntimeError("field '" + f.name + "' is decrypt: true but the run has no hsm-core client")
            try:
                self.transforms.append(methods.create(profile, f, keys=keys, clock=clock,
                                                      restricted_zip3=restricted_zip3,
                                                      surrogate_key_edek_id=surrogate_key_edek_id))
            except (ValueError, RuntimeError) as e:
                raise RuntimeError("field '" + f.name + "': " + str(e)) from e

    def surrogate_indexes(self) -> list[int]:
        return [i for i, t in enumerate(self.transforms) if isinstance(t, SurrogateTransform)]

    def close(self) -> None:
        if self._keys is not None:
            self._keys.close()

    # ---- one batch

    def process(self, records: list[dict]) -> BatchResult:
        stats = BatchStats(rows=len(records))
        values, hashes, encs = [], [], []
        for i, (f, t) in enumerate(zip(self.fields, self.transforms)):
            col_stats = methods.FieldStats()
            source = []
            for r in records:
                v = r.get(f.name)
                col_stats.count_value(v)
                if f.decrypt and v:
                    v = self._decrypt(v, f, r, stats)
                    col_stats.increment("decrypted")
                source.append(v)
            if isinstance(t, SurrogateTransform):
                h_col, e_col = self._surrogate_inputs(t, f, source, records)
                values.append([v if not v else None for v in source])
                hashes.append(h_col)
                encs.append(e_col)
            else:
                out = []
                for v, r in zip(source, records):
                    try:
                        out.append(t.apply(v, r, col_stats))
                    except DeidError as e:
                        raise self._located(e, f, r) from None
                values.append(out)
                hashes.append(None)
                encs.append(None)
            stats.values[i] = col_stats.values
            stats.empty[i] = col_stats.empty
            if col_stats.counters:
                stats.counters[i] = dict(col_stats.counters)
        return BatchResult(values, hashes, encs, stats)

    def rows(self, result: BatchResult, surrogates: list[dict] | None = None) -> list[list]:
        """Output rows, given per surrogate field a map of hash hex -> surrogate (the dry run's path)."""
        out = []
        for r in range(result.stats.rows):
            row = []
            for i in range(len(self.fields)):
                if result.hashes[i] is not None and result.hashes[i][r] is not None:
                    row.append(surrogates[i][result.hashes[i][r]])
                else:
                    row.append(result.values[i][r])
            out.append(row)
        return out

    def verify_decrypt(self, sample: list[dict]) -> None:
        """Decrypts the first non-empty value of each decrypt: true field, so a missing grant fails before writing."""
        for f in self.fields:
            if not f.decrypt:
                continue
            for r in sample:
                v = r.get(f.name)
                if v:
                    self._decrypt(v, f, r, BatchStats())
                    break

    def _surrogate_inputs(self, t: SurrogateTransform, f: Field, source: list, records: list[dict]):
        h_col, e_col = [], []
        enc_by_hash: dict[str, bytes] = {}
        for v, r in zip(source, records):
            if not v:
                h_col.append(None)
                e_col.append(None)
                continue
            try:
                normalized = t.normalized(v)
            except DeidError as e:
                raise self._located(e, f, r) from None
            h = t.lookup_hash(normalized).hex()
            enc = enc_by_hash.get(h)
            if enc is None:
                enc = enc_by_hash[h] = t.encrypt_value(normalized)
            h_col.append(h)
            e_col.append(enc)
        return h_col, e_col

    def _decrypt(self, value: str, f: Field, record: dict, stats: BatchStats) -> str:
        from .. import dek_manager
        token = _java.strip(value)
        try:
            unpacked = dek_manager.unpack_token(token)
        except ValueError as e:
            raise self._located(DeidError.bad_value("marked decrypt: true but not an HSM ciphertext (" + str(e) + ")"),
                                f, record) from None
        stats.edek_ids.add(str(unpacked.edek_id))
        try:
            return self._hsm.decrypt_to_string(token)
        except Exception as e:  # noqa: BLE001 - the reason names the key, never the value
            raise self._located(DeidError.bad_value("decrypt failed for edek_id=" + str(unpacked.edek_id) + ": "
                                                    + _safe(e)), f, record) from None

    def _located(self, e: DeidError, f: Field, record: dict) -> DeidError:
        key = record.get(self.key_column) if self.key_column else None
        where = "field '" + f.name + "'" + (", " + self.key_column + " " + key if key else "")
        return DeidError(where + ": " + e.reason)


def _safe(e: Exception) -> str:
    """An error's text when it is ours or hsm-core's (they carry no values); otherwise only its type."""
    from ..svc_client import SvcClientError
    if type(e).__name__ == "InvalidTag":
        return "ciphertext authentication failed"
    if isinstance(e, (DeidError, SvcClientError)) or type(e) is RuntimeError:
        return str(e)
    return type(e).__name__


def merge_counters(fields: list[Field], transforms: list, stats: BatchStats) -> list[dict]:
    """One manifest line per output field (DeidPipeline.report)."""
    out = []
    for i, (f, t) in enumerate(zip(fields, transforms)):
        out.append({"source": f.name, "phi": f.phi.label if f.phi else None, "method": f.method.id,
                    "settings": t.describe(), "decrypted": f.decrypt, "values": stats.values.get(i, 0),
                    "empty": stats.empty.get(i, 0), "counters": dict(stats.counters.get(i, {}))})
    return out
