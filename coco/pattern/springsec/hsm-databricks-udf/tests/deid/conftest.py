"""
Shared pieces for the deid tests: a fake hsm-core with the same keys as the Java
tests (DeidJobTestSupport), a local Spark session, and a fresh Postgres surrogate
table.

Spark and Postgres tests are skipped unless they're available: run
``tests/deid/spark/run-tests.sh`` to get both (in Docker).
"""

import datetime as dt
import os
import sys
import uuid
from pathlib import Path

import pytest

from hsm_databricks_udf import dek_manager

TOKEN_KEY_ID = uuid.UUID("6f1c0000-0000-4000-8000-000000000001")
SURROGATE_KEY_ID = uuid.UUID("6f1c0000-0000-4000-8000-000000000003")
COLUMN_DEK_ID = uuid.UUID("0c0c0000-0000-4000-8000-0000000000aa")
OWNER = "giftcard-svc"
RUN_TIME = dt.datetime(2026, 11, 20, 15, 30, tzinfo=dt.timezone.utc)
REPO = Path(__file__).resolve().parents[3]
POSTGRES_DDL = REPO / "java" / "hsm-bulk-client" / "src" / "main" / "resources" / "deid" / "deid-surrogate-postgres.sql"


def clock_2026():
    return RUN_TIME


def dek(seed: int) -> bytes:
    """As DeidJobTestSupport.dek: byte i = seed * 31 + i."""
    return bytes((seed * 31 + i) & 0xFF for i in range(32))


class FakeHsm:
    """hsm-core with the token key, the surrogate-table key and one column DEK. Picklable."""

    def __init__(self, deks=None):
        self.deks = dict(deks) if deks is not None else {TOKEN_KEY_ID: dek(1), SURROGATE_KEY_ID: dek(3),
                                                         COLUMN_DEK_ID: dek(9)}

    def encrypt(self, plaintext: str) -> str:
        r = dek_manager.encrypt(plaintext.encode("utf-8"), bytearray(self.deks[COLUMN_DEK_ID]), OWNER)
        return dek_manager.pack_token(COLUMN_DEK_ID, r.iv, r.tag, r.ciphertext)

    def unwrap_dek(self, edek_id):
        k = self.deks.get(uuid.UUID(str(edek_id)))
        if k is None:
            raise RuntimeError("dek/unwrap failed: no decrypt grant on edek_id")
        return k

    def decrypt_to_string(self, token: str) -> str:
        t = dek_manager.unpack_token(token)
        k = self.deks.get(t.edek_id)
        if k is None:
            raise RuntimeError("dek/unwrap failed: no decrypt grant on edek_id")
        return dek_manager.decrypt(t.ciphertext, t.tag, t.iv, bytearray(k), OWNER).decode("utf-8")


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    from pyspark import cloudpickle
    from pyspark.sql import SparkSession
    # The workers don't have this folder on their path: send the fakes by value.
    cloudpickle.register_pickle_by_value(sys.modules[__name__])
    session = (SparkSession.builder.master("local[2]").appName("deid-tests")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.execution.arrow.maxRecordsPerBatch", "3")  # several batches per partition
               .getOrCreate())
    yield session
    session.stop()


def pg_config(table="deid_surrogate"):
    from hsm_databricks_udf.deid.surrogate_store import DbConfig
    host = os.environ.get("DEID_TEST_PG_HOST")
    if not host:
        pytest.skip("no Postgres (DEID_TEST_PG_HOST); run tests/deid/spark/run-tests.sh")
    return DbConfig(host=host, database=os.environ.get("DEID_TEST_PG_DB", "deid"), user="postgres",
                    password=os.environ.get("DEID_TEST_PG_PASSWORD", ""), sslmode="disable", table=table)


@pytest.fixture
def pg():
    """A fresh deid_surrogate table (the shipped DDL) in its own schema; yields its DbConfig."""
    pytest.importorskip("psycopg")
    from hsm_databricks_udf.deid.surrogate_store import DbConfig, connect
    base = pg_config()
    schema = "t" + uuid.uuid4().hex[:12]
    conn = connect(base)
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA " + schema)
        cur.execute("SET search_path TO " + schema)
        for stmt in _ddl_statements():
            cur.execute(stmt)
    cfg = DbConfig(host=base.host, database=base.database, user=base.user, password=base.password,
                   sslmode=base.sslmode, schema=schema, table="deid_surrogate")
    yield cfg
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA " + schema + " CASCADE")
    conn.close()


def _ddl_statements():
    lines = [ln.split("--")[0] for ln in POSTGRES_DDL.read_text(encoding="utf-8").splitlines()]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def pg_count(cfg, where="TRUE"):
    from hsm_databricks_udf.deid.surrogate_store import connect
    conn = connect(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM " + cfg.qualified_table + " WHERE " + where)
            return cur.fetchone()[0]
    finally:
        conn.close()
