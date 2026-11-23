"""
The Databricks DEID job (java/docs/DEIDENTIFICATION.md section 17): one profile, one
Delta table or query, one CSV and its manifest in a volume folder.

A run (section 17.3):

0. checks before anything is written: the profile against the source's columns;
   the output file doesn't exist; keys unwrap; the surrogate table is usable; one
   sample value per ``decrypt: true`` field decrypts; a real run fails on unmarked
   ciphertext in the sample;
1-3. transforms on the workers, staged once to ``<output root>/_deid_tmp/<run id>/``;
4-5. surrogates resolved, counts added up;
6. the CSV lines written by one task, then streamed into ``<file>.csv.part`` with the
   SHA-256 and renamed; ``<file>.manifest.json``; one audit event;
7. the temporary folder deleted, also on failure.

A dry run reads ``sample`` rows on the driver, transforms them, prints the review
(section 10.3) and writes nothing, no surrogate rows either.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import re
import secrets
import shutil
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from . import csv_output, manifest as manifest_mod, pipeline, source as source_mod, spark_methods
from .errors import DeidError, DeidValidationError
from .pipeline import BatchProcessor, BatchStats
from .profile import DeidProfile, Method, parse_yaml
from .surrogate_store import DbConfig, SurrogateStore
from .validator import problems as profile_problems

log = logging.getLogger("hsm_databricks_udf.deid")
audit = logging.getLogger("hsm_databricks_udf.deid.audit")

CHECK_ROWS = 1000


class HsmCoreClient:
    """hsm-core for the job: unwrap by edek_id and decrypt tokens, through the package's
    per-worker DEK cache. Picklable: it connects on first use, in whichever process."""

    def __init__(self, credentials_json: str):
        self._credentials_json = credentials_json
        self._state = None

    def __getstate__(self):
        return {"_credentials_json": self._credentials_json, "_state": None}

    def _client(self):
        if self._state is None:
            from ..udf import _ensure_initialized
            self._state = _ensure_initialized(self._credentials_json)
        return self._state

    def unwrap_dek(self, edek_id: uuid.UUID) -> bytes:
        from .. import cache
        svc, key = self._client()
        _owner, dek = cache.get_or_unwrap_for_decrypt(str(edek_id), svc, key)
        return bytes(dek)

    def decrypt_to_string(self, token: str) -> str:
        from ..udf import decrypt
        return decrypt(token, self._credentials_json)

    def issue(self, dek_name: str):
        svc, _ = self._client()
        return svc.issue_dek(dek_name)


@dataclass
class Settings:
    profile_path: str
    output_root: str
    source_table: str | None = None
    source_query: str | None = None
    key_column: str | None = None
    surrogate_key_edek_id: str | None = None
    surrogate_db: DbConfig | None = None
    surrogate_partitions: int = 8
    dry_run: bool = False
    sample: int = 100
    triggered_by: str | None = None


@dataclass
class Result:
    csv: Path
    manifest_file: Path
    manifest: dict
    review: dict | None


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


class DeidJob:
    def __init__(self, spark, settings: Settings, *, hsm=None, store_factory: Callable | None = None,
                 clock: Callable[[], dt.datetime] = _utc_now, new_surrogate: Callable | None = None):
        """
        ``hsm``: an object with ``unwrap_dek`` and ``decrypt_to_string`` (``HsmCoreClient``),
        picklable; needed only when the profile calls hsm-core. ``store_factory(cfg)``
        opens a ``SurrogateStore``; picklable. ``new_surrogate`` replaces the random
        surrogate maker (tests only).
        """
        self.spark = spark
        self.s = settings
        self.hsm = hsm
        self.store_factory = store_factory or SurrogateStore.open
        self.clock = clock
        self.new_surrogate = new_surrogate

    # ------------------------------------------------------------------ run

    def run(self) -> Result:
        started = self.clock()
        run_id = "deid-" + started.astimezone(dt.timezone.utc).strftime("%Y%m%d-%H%M") + "-" + secrets.token_hex(2)
        profile_bytes = Path(self.s.profile_path).read_bytes()
        profile_sha = hashlib.sha256(profile_bytes).hexdigest()
        profile = parse_yaml(profile_bytes)
        log.info("deid_start run_id=%s profile=%s version=%s vendor=%s basis=%s dry_run=%s", run_id, profile.id,
                 profile.version, profile.vendor, profile.basis.label, self.s.dry_run)
        tmp = Path(self.s.output_root) / "_deid_tmp" / run_id
        store = None
        processor = None
        try:
            self.spark.conf.set("spark.sql.session.timeZone", "UTC")
            wanted = pipeline.columns_to_read(profile)
            if self.s.key_column and self.s.key_column not in wanted:
                wanted = wanted + [self.s.key_column]
            df, kinds, all_columns = source_mod.read(self.spark, table=self.s.source_table, query=self.s.source_query,
                                                     wanted=wanted)
            found = profile_problems(profile, all_columns)
            if found:
                for p in found:
                    log.error("deid_profile_problem run_id=%s profile=%s problem=%s", run_id, profile.id, p)
                raise DeidValidationError(found)

            file_name = output_file_name(profile, run_id, started)
            root = Path(self.s.output_root)
            csv_path = root / file_name
            manifest_path = root / manifest_name(file_name)
            if not self.s.dry_run:
                csv_output.check_target(csv_path)
                csv_output.check_target(manifest_path)

            key_id = self._surrogate_key_id(profile)
            if pipeline.needs_hsm(profile) and self.hsm is None:
                raise RuntimeError("the profile calls hsm-core (tokenize, surrogate or decrypt: true) but the job "
                                   "has no hsm-core credentials")
            processor = BatchProcessor(profile, hsm=self.hsm, clock=self.clock, surrogate_key_edek_id=key_id,
                                       key_column=self.s.key_column)
            if pipeline.needs_surrogate_table(profile):
                if self.s.surrogate_db is None:
                    raise RuntimeError("the profile uses surrogate: --surrogate-db settings are required")
                store = self.store_factory(self.s.surrogate_db)
                store.verify_available()

            check_rows = self.s.sample if self.s.dry_run else CHECK_ROWS
            sample = _records(df.limit(check_rows).collect(), kinds)
            processor.verify_decrypt(sample)
            unmarked = pipeline.unmarked_ciphertext(profile, sample)
            if unmarked and not self.s.dry_run:
                raise DeidValidationError(["field '" + f + "' holds HSM ciphertext but isn't decrypt: true"
                                           for f in unmarked])

            if self.s.dry_run:
                return self._dry_run(profile, processor, store, sample, unmarked, run_id, profile_sha, started,
                                     key_id, all_columns, csv_path, manifest_path)
            result = self._export(profile, processor, df, kinds, run_id, profile_sha, started, key_id, all_columns,
                                  csv_path, manifest_path, tmp)
            return result
        except BaseException as e:
            audit.error(json.dumps({"event": "deid_export_failed", "run_id": run_id, "profile": profile.id,
                                    "version": profile.version, "profile_sha256": profile_sha,
                                    "vendor": profile.vendor, "dry_run": self.s.dry_run, "reason": safe_reason(e)}))
            raise
        finally:
            if processor is not None:
                processor.close()
            if store is not None:
                store.close()
            shutil.rmtree(tmp, ignore_errors=True)
            try:
                tmp.parent.rmdir()  # _deid_tmp, when no other run is using it
            except OSError:
                pass

    def _surrogate_key_id(self, profile: DeidProfile) -> uuid.UUID | None:
        if not pipeline.needs_surrogate_table(profile):
            return None
        if not self.s.surrogate_key_edek_id:
            raise RuntimeError("the profile uses surrogate: --surrogate-key-edek-id is required; create the key "
                               "with --init-key")
        try:
            return uuid.UUID(self.s.surrogate_key_edek_id)
        except ValueError:
            raise RuntimeError("--surrogate-key-edek-id must be a UUID (an edek_id from --init-key)") from None

    # ------------------------------------------------------------------ the export

    def _export(self, profile, processor: BatchProcessor, df, kinds, run_id, profile_sha, started, key_id,
                all_columns, csv_path: Path, manifest_path: Path, tmp: Path) -> Result:
        s = self.s
        n = len(processor.fields)
        surrogate_idx = processor.surrogate_indexes()
        domains = {i: processor.transforms[i].domain for i in surrogate_idx}
        make_processor = _ProcessorFactory(profile, self.hsm, self.clock, key_id, s.key_column)

        stage1_path = str(tmp / "stage1")
        spark_methods.stage1(df, make_processor, kinds, n, surrogate_idx).write.parquet(stage1_path)
        staged = self.spark.read.parquet(stage1_path)
        stats = spark_methods.merged_stats(staged)

        surrogates = None
        counts = {}
        if surrogate_idx:
            surrogates_path = str(tmp / "surrogates")
            inputs = spark_methods.surrogate_inputs(staged, surrogate_idx, domains)
            spark_methods.resolve_surrogates(
                inputs, partitions=s.surrogate_partitions, open_store=_StoreOpener(self.store_factory, s.surrogate_db),
                scope=profile.surrogate_scope, key_edek_id=key_id, run_id=run_id,
                new_surrogate=self.new_surrogate or _NewSurrogate(processor.transforms[surrogate_idx[0]]),
                dry_run=False).write.parquet(surrogates_path)
            surrogates = self.spark.read.parquet(surrogates_path)
            counts = spark_methods.surrogate_counts(staged, surrogates, surrogate_idx, domains)

        out = profile.output
        rows = spark_methods.output_rows(staged, surrogates, n, surrogate_idx, domains)
        lines_path = str(tmp / "csv")
        (spark_methods.csv_lines(rows, n, out.delimiter, out.formula_guard).repartition(1)
         .write.option("lineSep", "\r\n" if out.crlf else "\n").text(lines_path))
        parts = sorted(Path(lines_path).glob("part-*"))
        sha = csv_output.assemble(csv_path, processor.header, parts, delimiter=out.delimiter, crlf=out.crlf)

        reports = _reports(processor, stats, counts)
        m = manifest_mod.build(profile, reports, manifest_mod.Run(
            run_id, profile_sha, s.triggered_by, started, self.clock(), key_id and str(key_id), all_columns,
            stats.rows, 0, csv_path.name, sha, False), decrypted_edek_ids=len(stats.edek_ids))
        csv_output.write_text(manifest_path, manifest_mod.to_json(m) + "\n")
        audit.info(json.dumps({"event": "deid_export_completed", "run_id": run_id, "profile": profile.id,
                               "version": profile.version, "profile_sha256": profile_sha, "vendor": profile.vendor,
                               "basis": profile.basis.label, "rows": stats.rows, "rows_skipped": 0,
                               "file": csv_path.name, "file_sha256": sha, "triggered_by": s.triggered_by}))
        return Result(csv_path, manifest_path, m, None)

    # ------------------------------------------------------------------ dry run

    def _dry_run(self, profile, processor: BatchProcessor, store, sample, unmarked, run_id, profile_sha, started,
                 key_id, all_columns, csv_path, manifest_path) -> Result:
        result = processor.process(sample)
        surrogate_idx = processor.surrogate_indexes()
        maps = [None] * len(processor.fields)
        counts = {}
        for i in surrogate_idx:
            t = processor.transforms[i]
            items = {}
            for h, e in zip(result.hashes[i], result.value_enc[i]):
                if h is not None:
                    items.setdefault(h, e)
            resolved = store.resolve(profile.surrogate_scope, t.domain, [(bytes.fromhex(h), e) for h, e in items.items()],
                                     key_edek_id=key_id, run_id=run_id,
                                     new_surrogate=self.new_surrogate or t.new_surrogate, dry_run=True)
            maps[i] = {h: s for h, (s, _) in resolved.items()}
            counts[i] = {"existing": sum(1 for _, n in resolved.values() if not n),
                         "new": sum(1 for _, n in resolved.values() if n)}
        rows = processor.rows(result, maps)
        reports = _reports(processor, result.stats, counts)
        m = manifest_mod.build(profile, reports, manifest_mod.Run(
            run_id, profile_sha, self.s.triggered_by, started, None, key_id and str(key_id), all_columns,
            result.stats.rows, 0, csv_path.name, None, True), decrypted_edek_ids=len(result.stats.edek_ids))

        fields = []
        for i, r in enumerate(reports):
            distinct = {row[i] for row in rows if row[i]}
            fields.append({"source": r["source"], "output": processor.header[i], "method": r["method"],
                           "phi": r["phi"],
                           "null_rate": 0.0 if not r["values"] else round(1000.0 * r["empty"] / r["values"]) / 1000.0,
                           "distinct_in_sample": len(distinct)})
        listed = {f.name for f in profile.fields}
        review = {"sample_rows": result.stats.rows, "fields": fields,
                  "passthrough": [f.name for f in processor.fields if f.method is Method.PASSTHROUGH],
                  "dropped_by_default": [c for c in all_columns if c not in listed],
                  "ciphertext_not_marked_decrypt": unmarked}
        print(manifest_mod.to_json({"manifest": m, "review": review}))
        log.info("deid_dry_run_complete run_id=%s profile=%s sample_rows=%s unmarked_ciphertext_fields=%s", run_id,
                 profile.id, result.stats.rows, len(unmarked))
        return Result(csv_path, manifest_path, m, review)


# ---------------------------------------------------------------------- helpers

class _ProcessorFactory:
    """Builds a BatchProcessor in each worker process; picklable."""

    def __init__(self, profile, hsm, clock, key_id, key_column):
        self.profile, self.hsm, self.clock, self.key_id, self.key_column = profile, hsm, clock, key_id, key_column

    def __call__(self) -> BatchProcessor:
        return BatchProcessor(self.profile, hsm=self.hsm, clock=self.clock, surrogate_key_edek_id=self.key_id,
                              key_column=self.key_column)


class _StoreOpener:
    def __init__(self, factory, cfg):
        self.factory, self.cfg = factory, cfg

    def __call__(self):
        return self.factory(self.cfg)


class _NewSurrogate:
    """A surrogate maker with the field's prefix and length; holds no keys."""

    def __init__(self, t):
        self.prefix, self.length = t.prefix, t.length

    def __call__(self) -> str:
        from .surrogate import CROCKFORD
        out = [self.prefix]
        for i in range(self.length):
            if i > 0 and i % 5 == 0:
                out.append("-")
            out.append(CROCKFORD[secrets.randbelow(len(CROCKFORD))])
        return "".join(out)


def _records(rows, kinds: dict) -> list[dict]:
    out = []
    for row in rows:
        d = row.asDict()
        out.append({c: source_mod.to_java_string(v, kinds.get(c, "plain")) for c, v in d.items()})
    return out


def _reports(processor: BatchProcessor, stats: BatchStats, surrogate_counts: dict) -> list[dict]:
    reports = pipeline.merge_counters(processor.fields, processor.transforms, stats)
    for i, r in enumerate(reports):
        r["output"] = processor.header[i]
        if i in surrogate_counts:
            c = surrogate_counts[i]
            r["counters"]["existing"] = c["existing"]
            if c["new"]:
                r["counters"]["new"] = c["new"]
    return reports


def output_file_name(profile: DeidProfile, run_id: str, started: dt.datetime) -> str:
    """``output.file`` with ``{yyyyMMdd}``, ``{run}`` and ``{profile}`` filled in (as the Java job)."""
    name = (profile.output.file.replace("{yyyyMMdd}", started.astimezone(dt.timezone.utc).strftime("%Y%m%d"))
            .replace("{run}", run_id).replace("{profile}", profile.id))
    if "/" in name or "\\" in name or name.startswith(".") or "{" in name:
        raise RuntimeError("output.file must be a plain file name with only the {yyyyMMdd}, {run} and {profile} "
                           "placeholders")
    return name


def manifest_name(csv_name: str) -> str:
    base = csv_name[:-4] if csv_name.lower().endswith(".csv") else csv_name
    return base + ".manifest.json"


_DEID_ERROR = re.compile(r"(?:DeidError|DeidValidationError): ([^\n]+)")


def safe_reason(e: BaseException) -> str:
    """The audit reason: our own messages (no values), else only the error's type."""
    if isinstance(e, DeidValidationError):
        return str(len(e.problems)) + " profile problem(s)"
    if isinstance(e, (DeidError, RuntimeError)) and type(e).__module__.startswith(("hsm_databricks_udf", "builtins")):
        return str(e)
    # A worker's DeidError arrives inside Spark's exception text.
    m = _DEID_ERROR.search(str(e))
    if m:
        return m.group(1)
    return type(e).__name__


def main_log_setup() -> None:
    """Plain lines on stdout: the job's log and the audit events (JSON)."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    for name in ("hsm_databricks_udf.deid",):
        lg = logging.getLogger(name)
        lg.setLevel(logging.INFO)
        if not lg.handlers:
            lg.addHandler(handler)
