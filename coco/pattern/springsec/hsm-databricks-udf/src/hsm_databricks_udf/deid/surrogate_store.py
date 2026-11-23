"""
The surrogate table on Postgres or Lakebase (java/docs/DEIDENTIFICATION.md sections 8
and 17.4): the same ``deid_surrogate`` table, from the same DDL
(``deid-surrogate-postgres.sql``), that the Java job uses.

``resolve`` maps a partition's distinct lookup hashes to surrogates:

1. reads the existing rows, in chunks;
2. inserts the missing ones with new random surrogates, ``INSERT … ON CONFLICT DO
   NOTHING``, in batches; a row that wasn't inserted either exists now (another run
   won the race) or its surrogate was taken;
3. re-reads those; the ones still missing had their surrogate taken and are retried
   with a new one, up to 5 times (section 8.3), then the run fails.

Two runs at once still agree: the primary key and the unique surrogate constraint
decide. Works with psycopg (3) or psycopg2, whichever is installed.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import Callable

from .errors import DeidError
from .surrogate import MAX_COLLISION_RETRIES

FIND_CHUNK = 1000
INSERT_CHUNK = 500
_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}")


@dataclass(frozen=True)
class DbConfig:
    """Connection settings; picklable, so the workers can open their own connection."""

    host: str
    database: str
    user: str
    password: str
    port: int = 5432
    sslmode: str = "require"
    schema: str | None = None
    table: str = "deid_surrogate"

    def __post_init__(self):
        for name in (self.schema, self.table):
            if name is not None and not _IDENT.fullmatch(name):
                raise ValueError("surrogate table and schema must be plain identifiers")

    def __repr__(self) -> str:  # never print the password
        return ("DbConfig(host=" + self.host + ", database=" + self.database + ", user=" + self.user
                + ", schema=" + str(self.schema) + ", table=" + self.table + ")")

    @property
    def qualified_table(self) -> str:
        return (self.schema + "." if self.schema else "") + self.table


def connect(cfg: DbConfig):
    """A DB-API connection in autocommit mode (psycopg 3, else psycopg2)."""
    kwargs = dict(host=cfg.host, port=cfg.port, dbname=cfg.database, user=cfg.user, password=cfg.password,
                  sslmode=cfg.sslmode, connect_timeout=15)
    try:
        import psycopg
        conn = psycopg.connect(**kwargs)
    except ImportError:
        import psycopg2
        conn = psycopg2.connect(**kwargs)
    conn.autocommit = True
    return conn


class SurrogateStore:
    def __init__(self, conn, cfg: DbConfig):
        self._conn = conn
        self._cfg = cfg
        self._table = cfg.qualified_table

    @classmethod
    def open(cls, cfg: DbConfig) -> "SurrogateStore":
        return cls(connect(cfg), cfg)

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:  # noqa: BLE001 - closing on the way out
            pass

    def verify_available(self) -> None:
        """Fails clearly when the table is missing or unusable; the job never creates it."""
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT scope, domain, lookup_hash, value_enc, key_edek_id, surrogate, created_at, "
                            "created_run_id FROM " + self._table + " WHERE 1 = 0")
        except Exception as e:  # noqa: BLE001 - reported without detail that could carry data
            raise DeidError.bad_value("the surrogate table " + self._table + " is missing or unusable ("
                                      + type(e).__name__ + "); the owner's DBA creates it from "
                                      "deid/deid-surrogate-postgres.sql (the job never creates tables)") from None

    def find(self, scope: str, domain: str, hashes: list[bytes]) -> dict[str, str]:
        """Existing surrogates by lookup hash (hex)."""
        out: dict[str, str] = {}
        with self._conn.cursor() as cur:
            for i in range(0, len(hashes), FIND_CHUNK):
                chunk = hashes[i:i + FIND_CHUNK]
                cur.execute("SELECT lookup_hash, surrogate FROM " + self._table
                            + " WHERE scope = %s AND domain = %s AND lookup_hash = ANY(%s)", (scope, domain, chunk))
                for h, s in cur.fetchall():
                    out[bytes(h).hex()] = s
        return out

    def _insert(self, scope: str, domain: str, rows: list[tuple[bytes, bytes, str]], key_edek_id: uuid.UUID,
                run_id: str) -> set[str]:
        """Inserts (hash, value_enc, surrogate) rows; returns the hex hashes actually inserted."""
        inserted: set[str] = set()
        with self._conn.cursor() as cur:
            for i in range(0, len(rows), INSERT_CHUNK):
                chunk = rows[i:i + INSERT_CHUNK]
                placeholders = ", ".join(["(%s, %s, %s, %s, CAST(%s AS uuid), %s, %s)"] * len(chunk))
                params = []
                for h, enc, s in chunk:
                    params.extend([scope, domain, h, enc, str(key_edek_id), s, run_id])
                cur.execute("INSERT INTO " + self._table + " (scope, domain, lookup_hash, value_enc, key_edek_id, "
                            "surrogate, created_run_id) VALUES " + placeholders
                            + " ON CONFLICT DO NOTHING RETURNING lookup_hash", params)
                inserted.update(bytes(r[0]).hex() for r in cur.fetchall())
        return inserted

    def resolve(self, scope: str, domain: str, items: list[tuple[bytes, bytes]], *, key_edek_id: uuid.UUID,
                run_id: str, new_surrogate: Callable[[], str], dry_run: bool = False) -> dict[str, tuple[str, bool]]:
        """
        ``items``: distinct (lookup_hash, value_enc). Returns hash hex -> (surrogate, created
        by this run). A dry run inserts nothing: new surrogates exist only in the result.
        """
        by_hex = {h.hex(): (h, enc) for h, enc in items}
        found = self.find(scope, domain, [h for h, _ in by_hex.values()])
        result = {k: (s, False) for k, s in found.items()}
        missing = [k for k in by_hex if k not in result]
        if dry_run:
            taken = set(found.values())
            for k in missing:
                s = new_surrogate()
                while s in taken:
                    s = new_surrogate()
                taken.add(s)
                result[k] = (s, True)
            return result
        for _attempt in range(MAX_COLLISION_RETRIES):
            if not missing:
                return result
            proposed = {k: new_surrogate() for k in missing}
            inserted = self._insert(scope, domain, [(by_hex[k][0], by_hex[k][1], proposed[k]) for k in missing],
                                    key_edek_id, run_id)
            for k in inserted:
                result[k] = (proposed[k], True)
            others = [k for k in missing if k not in inserted]
            if others:
                reread = self.find(scope, domain, [by_hex[k][0] for k in others])
                for k, s in reread.items():
                    result[k] = (s, False)
            missing = [k for k in others if k not in result]
        if missing:
            raise DeidError.bad_value("could not find an unused surrogate after " + str(MAX_COLLISION_RETRIES)
                                      + " tries; increase the surrogate length")
        return result
