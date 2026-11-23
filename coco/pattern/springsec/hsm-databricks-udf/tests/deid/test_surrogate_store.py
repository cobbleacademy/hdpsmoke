"""The surrogate table on a real Postgres, with the shipped DDL (skipped without one: tests/deid/spark/run-tests.sh)."""

import threading
import uuid

import pytest

from hsm_databricks_udf.deid.errors import DeidError
from hsm_databricks_udf.deid.surrogate_store import DbConfig, SurrogateStore

from conftest import SURROGATE_KEY_ID, pg_count


def h(n):
    return bytes([n % 256, n // 256]) + bytes(30)


class Seq:
    """Surrogates from a list, then random-looking ones."""

    def __init__(self, *first):
        self.first = list(first)
        self.n = 0

    def __call__(self):
        if self.first:
            return self.first.pop(0)
        self.n += 1
        return "S-" + uuid.uuid4().hex[:10].upper()


def test_insert_find_and_reuse(pg):
    store = SurrogateStore.open(pg)
    try:
        store.verify_available()
        r = store.resolve("zen", "member", [(h(1), b"enc1"), (h(2), b"enc2")], key_edek_id=SURROGATE_KEY_ID,
                          run_id="run-1", new_surrogate=Seq("S-A", "S-B"))
        assert r == {h(1).hex(): ("S-A", True), h(2).hex(): ("S-B", True)}
        again = store.resolve("zen", "member", [(h(1), b"x"), (h(3), b"enc3")], key_edek_id=SURROGATE_KEY_ID,
                              run_id="run-2", new_surrogate=Seq("S-C"))
        assert again == {h(1).hex(): ("S-A", False), h(3).hex(): ("S-C", True)}
        assert store.find("acme", "member", [h(1)]) == {}, "scopes are separate"
        assert pg_count(pg, "created_run_id = 'run-1' AND key_edek_id = '" + str(SURROGATE_KEY_ID) + "'") == 2
        assert pg_count(pg) == 3
    finally:
        store.close()


def test_a_taken_surrogate_is_retried_and_another_domain_may_reuse_it(pg):
    store = SurrogateStore.open(pg)
    try:
        store.resolve("zen", "member", [(h(1), b"e")], key_edek_id=SURROGATE_KEY_ID, run_id="r",
                      new_surrogate=Seq("S-A"))
        r = store.resolve("zen", "member", [(h(2), b"e")], key_edek_id=SURROGATE_KEY_ID, run_id="r",
                          new_surrogate=Seq("S-A", "S-A", "S-B"))
        assert r == {h(2).hex(): ("S-B", True)}
        assert store.resolve("zen", "card", [(h(2), b"e")], key_edek_id=SURROGATE_KEY_ID, run_id="r",
                             new_surrogate=Seq("S-A")) == {h(2).hex(): ("S-A", True)}
        with pytest.raises(DeidError, match="could not find an unused surrogate after 5 tries"):
            store.resolve("zen", "member", [(h(9), b"e")], key_edek_id=SURROGATE_KEY_ID, run_id="r",
                          new_surrogate=lambda: "S-A")
    finally:
        store.close()


def test_many_hashes_in_chunks(pg):
    store = SurrogateStore.open(pg)
    try:
        items = [(h(i), b"e") for i in range(2600)]
        r = store.resolve("zen", "member", items, key_edek_id=SURROGATE_KEY_ID, run_id="r", new_surrogate=Seq())
        assert len(r) == 2600 and all(new for _, new in r.values())
        assert len(store.find("zen", "member", [x for x, _ in items])) == 2600
    finally:
        store.close()


def test_concurrent_runs_agree(pg):
    items = [(h(i), b"e") for i in range(400)]
    results = []

    def run(n):
        store = SurrogateStore.open(pg)
        try:
            results.append(store.resolve("zen", "member", items, key_edek_id=SURROGATE_KEY_ID, run_id="run-" + str(n),
                                         new_surrogate=Seq()))
        finally:
            store.close()

    threads = [threading.Thread(target=run, args=(n,)) for n in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    maps = [{k: s for k, (s, _) in r.items()} for r in results]
    assert len(results) == 4 and all(m == maps[0] for m in maps)
    assert sum(new for r in results for _, new in r.values()) == 400, "each value created by exactly one run"
    assert pg_count(pg) == 400


def test_a_dry_run_inserts_nothing(pg):
    store = SurrogateStore.open(pg)
    try:
        store.resolve("zen", "member", [(h(1), b"e")], key_edek_id=SURROGATE_KEY_ID, run_id="r",
                      new_surrogate=Seq("S-A"))
        r = store.resolve("zen", "member", [(h(1), b"e"), (h(2), b"e")], key_edek_id=SURROGATE_KEY_ID, run_id="r",
                          new_surrogate=Seq("S-A", "S-B"), dry_run=True)
        assert r == {h(1).hex(): ("S-A", False), h(2).hex(): ("S-B", True)}, "a taken surrogate is skipped"
        assert pg_count(pg) == 1
    finally:
        store.close()


def test_a_missing_table_fails_clearly(pg):
    cfg = DbConfig(host=pg.host, database=pg.database, user=pg.user, password=pg.password, sslmode=pg.sslmode,
                   schema=pg.schema, table="no_such_table")
    store = SurrogateStore.open(cfg)
    try:
        with pytest.raises(DeidError, match="deid/deid-surrogate-postgres.sql"):
            store.verify_available()
    finally:
        store.close()


def test_names_must_be_plain_identifiers_and_the_password_never_prints():
    with pytest.raises(ValueError):
        DbConfig(host="h", database="d", user="u", password="p", table="x; DROP TABLE y")
    assert "secret-pw" not in repr(DbConfig(host="h", database="d", user="u", password="secret-pw"))
