"""
``hsm-deid``: the Databricks DEID job's entry point (java/docs/DEIDENTIFICATION.md
section 17.6), for a Python wheel task on serverless or a dedicated cluster.

Secrets come from a Databricks secret scope, read on the driver (the workers can't
read secrets themselves). hsm-core's credentials either:

- as separate values, like the encrypt/decrypt functions' (``DEPLOYMENT.md`` §0):
  ``--hsm-base-url``, ``--hsm-app-id``, ``--hsm-auth-mode``, ``--hsm-api-version``
  and the secret-scope keys ``--hsm-private-key-key``, ``--hsm-token-key`` (STATIC),
  ``--hsm-signing-key-key`` (SELF_SIGNED_JWT), ``--hsm-ca-cert-key`` (optional),
  ``--hsm-jwt-audience`` (SELF_SIGNED_JWT, when hsm-core's audience isn't the default);
- or as one secret holding the whole ``HSM_*`` JSON object: ``--hsm-credentials-key``.

``--surrogate-db-password-key``: the surrogate database password (a Lakebase
role's password, or a token).

Outside Databricks (local runs, tests) the values can come from the environment:
``HSM_DEID_CREDENTIALS_JSON`` and ``HSM_DEID_DB_PASSWORD``.

Exits non-zero on any failure, so the Databricks task fails with it.

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
    p.add_argument("--hsm-credentials-key", help="secret-scope key holding the whole HSM_* credentials JSON")
    p.add_argument("--hsm-base-url", help="hsm-core-service base URL, ending in /v1 or /v2")
    p.add_argument("--hsm-app-id", help="this job's app_id in hsm-core")
    p.add_argument("--hsm-auth-mode", choices=["STATIC", "SELF_SIGNED_JWT"], default="STATIC")
    p.add_argument("--hsm-api-version", choices=["V1", "V2"], default="V1")
    p.add_argument("--hsm-private-key-key", help="secret-scope key: the DEK-transport private key (PEM or base64)")
    p.add_argument("--hsm-token-key", help="secret-scope key: the bearer token (STATIC)")
    p.add_argument("--hsm-signing-key-key", help="secret-scope key: the JWT signing key (SELF_SIGNED_JWT)")
    p.add_argument("--hsm-ca-cert-key", help="secret-scope key: an internal CA bundle for hsm-core's TLS (optional)")
    p.add_argument("--hsm-jwt-audience", help="SELF_SIGNED_JWT: the token's aud, hsm-core's hsm.jwt.audience "
                                              "(default hsm-core-service)")
    p.add_argument("--triggered-by")
    p.add_argument("--dry-run", action="store_true", help="read --sample rows, print the review, write nothing")
    p.add_argument("--sample", type=int, default=100)
    p.add_argument("--init-key", action="store_true", help="create a key and print its edek_id")
    p.add_argument("--name", help="with --init-key: the key name, deid-<…>")
    return p


def _dbutils():
    """dbutils in a Databricks job: databricks-sdk's runtime one (serverless), else pyspark's (classic)."""
    try:
        from databricks.sdk.runtime import dbutils
        return dbutils
    except ImportError:
        pass
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        return DBUtils(SparkSession.builder.getOrCreate())
    except ImportError:
        return None


def _secret(scope: str, key: str | None, env: str | None = None) -> str | None:
    if env and os.environ.get(env):
        return os.environ[env]
    if not key:
        return None
    dbutils = _dbutils()
    if dbutils is None:
        raise SystemExit("secret '" + key + "' can't be read outside Databricks"
                         + ("; set " + env if env else "")) from None
    return dbutils.secrets.get(scope=scope, key=key)


def credentials_json(args, secret=_secret) -> str | None:
    """hsm-core's credentials as the HSM_* JSON object the package's Config reads, or None."""
    whole = secret(args.secret_scope, args.hsm_credentials_key, "HSM_DEID_CREDENTIALS_JSON")
    if whole:
        if args.hsm_jwt_audience:
            raise SystemExit("--hsm-jwt-audience is for the separate secrets; with --hsm-credentials-key "
                             "put HSM_SELF_SIGNED_AUDIENCE in the JSON")
        return whole
    if args.hsm_jwt_audience and args.hsm_auth_mode != "SELF_SIGNED_JWT":
        raise SystemExit("--hsm-jwt-audience needs --hsm-auth-mode SELF_SIGNED_JWT")
    if not (args.hsm_base_url or args.hsm_app_id or args.hsm_private_key_key):
        return None
    if not (args.hsm_base_url and args.hsm_app_id and args.hsm_private_key_key):
        raise SystemExit("hsm-core credentials need --hsm-base-url, --hsm-app-id and --hsm-private-key-key")
    fields = {"HSM_SERVICE_BASE_URL": args.hsm_base_url, "HSM_APP_ID": args.hsm_app_id,
              "HSM_AUTH_MODE": args.hsm_auth_mode, "HSM_API_VERSION": args.hsm_api_version,
              "HSM_PRIVATE_KEY_PEM": secret(args.secret_scope, args.hsm_private_key_key)}
    if args.hsm_auth_mode == "STATIC":
        if not args.hsm_token_key:
            raise SystemExit("--hsm-auth-mode STATIC needs --hsm-token-key")
        fields["HSM_BEARER_TOKEN"] = secret(args.secret_scope, args.hsm_token_key)
    else:
        if args.hsm_signing_key_key:
            fields["HSM_SIGNING_PRIVATE_KEY_PEM"] = secret(args.secret_scope, args.hsm_signing_key_key)
        if args.hsm_jwt_audience:
            fields["HSM_SELF_SIGNED_AUDIENCE"] = args.hsm_jwt_audience
    if args.hsm_ca_cert_key:
        fields["HSM_CA_CERT_PEM"] = secret(args.secret_scope, args.hsm_ca_cert_key)
    return json.dumps(fields)


def main(argv: list[str] | None = None) -> None:
    """The console entry point: returns on success, raises SystemExit(status) on failure.

    Databricks runs a wheel task's entry point inside IPython, which reports any
    SystemExit, even sys.exit(0), as an exception and fails the task. So a
    successful run returns normally (a console script still exits 0), and only a
    failed run (1) or a bad parameter (2) raises, which fails the task.
    """
    status = run(argv)
    if status:
        raise SystemExit(status)


def run(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    main_log_setup()
    credentials = credentials_json(args)

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
    main()
