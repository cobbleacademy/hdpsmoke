"""
``hsm-deid``: the Databricks DEID job's entry point (java/docs/DEIDENTIFICATION.md
section 17.6), for a Python wheel task on serverless or a dedicated cluster.

Secrets come from a Databricks secret scope, read on the driver with
``dbutils.secrets.get`` (the workers can't read secrets themselves):

- ``--hsm-credentials-key``: a JSON object with the ``HSM_*`` fields, as for the
  encrypt/decrypt functions (``DEPLOYMENT.md``);
- ``--surrogate-db-password-key``: the surrogate database password (a Lakebase
  role's password, or a token).

Outside Databricks (local runs, tests) the same values can come from the
environment: ``HSM_DEID_CREDENTIALS_JSON`` and ``HSM_DEID_DB_PASSWORD``.

``--init-key --name deid-…`` creates a key with ``/dek/issue`` and prints its edek_id.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

from .job import DeidJob, HsmCoreClient, Settings, main_log_setup, safe_reason
from .surrogate_store import DbConfig

_KEY_NAME = re.compile(r"deid-[a-z0-9][a-z0-9-]*[a-z0-9]")


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="hsm-deid", description="De-identified CSV export from a Delta table.")
    p.add_argument("--profile", help="profile YAML, e.g. /Volumes/<catalog>/<schema>/deid/profiles/zen.yml")
    src = p.add_mutually_exclusive_group()
    src.add_argument("--source-table", help="<catalog>.<schema>.<table>")
    src.add_argument("--source-query", help="a SELECT statement instead of a table")
    p.add_argument("--key-column", help="a non-sensitive column named in errors, e.g. id")
    p.add_argument("--output-root", help="the volume folder for this vendor profile")
    p.add_argument("--surrogate-key-edek-id", help="edek_id of deid-<app>-surrogate (profiles using surrogate)")
    p.add_argument("--surrogate-db", choices=["postgres", "lakebase"], help="where deid_surrogate lives")
    p.add_argument("--surrogate-db-host")
    p.add_argument("--surrogate-db-port", type=int, default=5432)
    p.add_argument("--surrogate-db-name", help="database name")
    p.add_argument("--surrogate-db-user")
    p.add_argument("--surrogate-db-sslmode", default="require")
    p.add_argument("--surrogate-db-schema")
    p.add_argument("--surrogate-db-table", default="deid_surrogate")
    p.add_argument("--surrogate-db-password-key", help="secret-scope key holding the database password")
    p.add_argument("--surrogate-partitions", type=int, default=8, help="caps the database connections (default 8)")
    p.add_argument("--secret-scope", default="hsm")
    p.add_argument("--hsm-credentials-key", help="secret-scope key holding the HSM_* credentials JSON")
    p.add_argument("--triggered-by")
    p.add_argument("--dry-run", action="store_true", help="read --sample rows, print the review, write nothing")
    p.add_argument("--sample", type=int, default=100)
    p.add_argument("--init-key", action="store_true", help="create a key and print its edek_id")
    p.add_argument("--name", help="with --init-key: the key name, deid-<…>")
    return p


def _secret(scope: str, key: str | None, env: str) -> str | None:
    if os.environ.get(env):
        return os.environ[env]
    if not key:
        return None
    try:
        from pyspark.dbutils import DBUtils  # Databricks only
        from pyspark.sql import SparkSession
        return DBUtils(SparkSession.builder.getOrCreate()).secrets.get(scope=scope, key=key)
    except ImportError:
        raise SystemExit("secret '" + key + "' can't be read outside Databricks; set " + env) from None


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    main_log_setup()
    credentials = _secret(args.secret_scope, args.hsm_credentials_key, "HSM_DEID_CREDENTIALS_JSON")

    if args.init_key:
        if not args.name or not _KEY_NAME.fullmatch(args.name):
            print("--init-key needs --name deid-<…> (lower case, digits, hyphens)", file=sys.stderr)
            return 2
        if not credentials:
            print("--init-key needs hsm-core credentials (--hsm-credentials-key)", file=sys.stderr)
            return 2
        r = HsmCoreClient(credentials).issue(args.name)
        print(json.dumps({"edek_id": r.edek_id, "owner_app_id": r.owner_app_id, "reused": r.reused}))
        return 0

    for flag in ("profile", "output_root"):
        if not getattr(args, flag):
            print("--" + flag.replace("_", "-") + " is required", file=sys.stderr)
            return 2
    if not args.source_table and not args.source_query:
        print("--source-table or --source-query is required", file=sys.stderr)
        return 2

    db = None
    if args.surrogate_db:
        password = _secret(args.secret_scope, args.surrogate_db_password_key, "HSM_DEID_DB_PASSWORD")
        if not (args.surrogate_db_host and args.surrogate_db_name and args.surrogate_db_user and password):
            print("--surrogate-db needs --surrogate-db-host, -name, -user and a password", file=sys.stderr)
            return 2
        db = DbConfig(host=args.surrogate_db_host, port=args.surrogate_db_port, database=args.surrogate_db_name,
                      user=args.surrogate_db_user, password=password, sslmode=args.surrogate_db_sslmode,
                      schema=args.surrogate_db_schema, table=args.surrogate_db_table)

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    settings = Settings(profile_path=args.profile, output_root=args.output_root, source_table=args.source_table,
                        source_query=args.source_query, key_column=args.key_column,
                        surrogate_key_edek_id=args.surrogate_key_edek_id, surrogate_db=db,
                        surrogate_partitions=args.surrogate_partitions, dry_run=args.dry_run, sample=args.sample,
                        triggered_by=args.triggered_by)
    try:
        DeidJob(spark, settings, hsm=HsmCoreClient(credentials) if credentials else None).run()
    except Exception as e:  # noqa: BLE001 - the audit event has the reason; no values in it
        print("hsm-deid failed: " + safe_reason(e), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
