"""
The Spark side of a run (java/docs/DEIDENTIFICATION.md sections 17.3 and 17.4): the
functions the workers run, all through ``mapInPandas`` and the DataFrame API, so the
same code runs on serverless (Spark Connect: no RDDs, accumulators or ``cache()``) and
on a dedicated cluster.

- ``stage1``: per Arrow batch, the profile's transforms (``pipeline.BatchProcessor``);
  one extra row per batch carries that batch's counts as JSON. Surrogate fields carry
  the lookup hash and ``value_enc``, never the value, so nothing written to the
  temporary folder or shuffled holds an identifier.
- ``resolve_surrogates``: the distinct hashes, grouped so each is in one partition,
  resolved against the surrogate table, one connection per partition.
- ``csv_lines``: the output rows as CSV lines (``csv_output.line``).

The functions the workers run take only picklable settings: the profile, an hsm-core
client that connects on first use, and the database settings.
"""

from __future__ import annotations

import json
from typing import Callable

from . import csv_output, source as source_mod
from .pipeline import BatchProcessor, BatchStats

KIND = "__kind"
STATS = "__stats"


def stage1_schema(n_fields: int, surrogate_idx: list[int]) -> str:
    cols = ["`" + KIND + "` string"]
    for i in range(n_fields):
        cols.append("c" + str(i) + " string")
        if i in surrogate_idx:
            cols.append("h" + str(i) + " string")
            cols.append("e" + str(i) + " binary")
    cols.append("`" + STATS + "` string")
    return ", ".join(cols)


def records_of(pdf, kinds: dict) -> list[dict]:
    """A pandas batch as records of Java-form strings (``source.to_java_string``)."""
    fix = {c: k for c, k in kinds.items() if k != "plain"}
    records = pdf.to_dict("records")
    for r in records:
        for c, v in r.items():
            if v is not None and not isinstance(v, str):
                r[c] = None  # pandas' missing-value markers; every column was cast to string
        for c, k in fix.items():
            r[c] = source_mod.to_java_string(r.get(c), k)
    return records


def stage1(df, make_processor: Callable[[], BatchProcessor], kinds: dict, n_fields: int, surrogate_idx: list[int]):
    schema = stage1_schema(n_fields, surrogate_idx)

    def run(batches):
        import pandas as pd
        proc = make_processor()
        try:
            for pdf in batches:
                records = records_of(pdf, kinds)
                result = proc.process(records)
                out = {KIND: ["row"] * len(records)}
                for i in range(n_fields):
                    out["c" + str(i)] = result.values[i]
                    if i in surrogate_idx:
                        out["h" + str(i)] = result.hashes[i]
                        out["e" + str(i)] = result.value_enc[i]
                out[STATS] = [None] * len(records)
                rows = pd.DataFrame(out)
                stats = {c: [None] for c in out}
                stats[KIND] = ["stats"]
                stats[STATS] = [json.dumps(result.stats.to_json_dict())]
                yield pd.concat([rows, pd.DataFrame(stats)], ignore_index=True)
        finally:
            proc.close()

    return df.mapInPandas(run, schema)


def merged_stats(stage1_df) -> BatchStats:
    total = BatchStats()
    for row in stage1_df.where(stage1_df[KIND] == "stats").select(STATS).collect():
        total.add(BatchStats.from_json_dict(json.loads(row[0])))
    return total


def surrogate_inputs(stage1_df, surrogate_idx: list[int], domains: dict):
    """Distinct (domain, hash, one value_enc) across every surrogate field."""
    from pyspark.sql import functions as F
    parts = [stage1_df.where(F.col("h" + str(i)).isNotNull())
             .select(F.lit(domains[i]).alias("domain"), F.col("h" + str(i)).alias("hash"),
                     F.col("e" + str(i)).alias("enc"))
             for i in surrogate_idx]
    union = parts[0]
    for p in parts[1:]:
        union = union.unionByName(p)
    return union.groupBy("domain", "hash").agg(F.first("enc").alias("enc"))


def resolve_surrogates(inputs_df, *, partitions: int, open_store: Callable, scope: str, key_edek_id,
                       run_id: str, new_surrogate: Callable[[], str], dry_run: bool):
    schema = "domain string, hash string, surrogate string, is_new boolean"

    def run(batches):
        import pandas as pd
        store = open_store()
        try:
            for pdf in batches:
                out = {"domain": [], "hash": [], "surrogate": [], "is_new": []}
                for domain, group in pdf.groupby("domain"):
                    items = [(bytes.fromhex(h), bytes(e)) for h, e in zip(group["hash"], group["enc"])]
                    resolved = store.resolve(scope, domain, items, key_edek_id=key_edek_id, run_id=run_id,
                                             new_surrogate=new_surrogate, dry_run=dry_run)
                    for h in group["hash"]:
                        s, is_new = resolved[h]
                        out["domain"].append(domain)
                        out["hash"].append(h)
                        out["surrogate"].append(s)
                        out["is_new"].append(is_new)
                yield pd.DataFrame(out)
        finally:
            store.close()

    return inputs_df.repartition(partitions, "domain", "hash").mapInPandas(run, schema)


def output_rows(stage1_df, surrogates_df, n_fields: int, surrogate_idx: list[int], domains: dict):
    """The rows, one column ``o<i>`` per output field, surrogates joined in."""
    from pyspark.sql import functions as F
    rows = stage1_df.where(F.col(KIND) == "row")
    for i in surrogate_idx:
        s = (surrogates_df.where(F.col("domain") == domains[i])
             .select(F.col("hash").alias("jh" + str(i)), F.col("surrogate").alias("s" + str(i))))
        rows = rows.join(s, rows["h" + str(i)] == s["jh" + str(i)], "left")
    cols = []
    for i in range(n_fields):
        if i in surrogate_idx:
            cols.append(F.coalesce(F.col("s" + str(i)), F.col("c" + str(i))).alias("o" + str(i)))
        else:
            cols.append(F.col("c" + str(i)).alias("o" + str(i)))
    return rows.select(*cols)


def csv_lines(rows_df, n_fields: int, delimiter: str, guard: bool):
    def run(batches):
        import pandas as pd
        for pdf in batches:
            values = pdf[["o" + str(i) for i in range(n_fields)]].astype(object).where(pdf.notna(), None)
            yield pd.DataFrame({"line": [csv_output.line(list(r), delimiter, guard)
                                         for r in values.itertuples(index=False, name=None)]})

    return rows_df.mapInPandas(run, "line string")


def surrogate_counts(stage1_df, surrogates_df, surrogate_idx: list[int], domains: dict) -> dict:
    """Per surrogate field: distinct values new to the table this run, and those already in it."""
    from pyspark.sql import functions as F
    out = {}
    for i in surrogate_idx:
        distinct = stage1_df.where(F.col("h" + str(i)).isNotNull()).select(F.col("h" + str(i)).alias("hash")).distinct()
        s = surrogates_df.where(F.col("domain") == domains[i]).select("hash", "is_new")
        counts = {bool(r[0]): r[1] for r in distinct.join(s, "hash").groupBy("is_new").count().collect()}
        out[i] = {"existing": counts.get(False, 0), "new": counts.get(True, 0)}
    return out
