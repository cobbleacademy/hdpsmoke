#!/usr/bin/env python3
"""
Launch hsm-bulk-client.jar for a BULK File job: encrypt (or decrypt) every file under a
folder -- an Azure Blob / ADLS Gen2 folder, or a local one -- into a target folder.

The jar does all the work (lists the files, gets DEKs from hsm-core-service /
hsm-bulk-service via /dek/issue or /dek/unwrap, AES-256-GCM per chunk locally, writes the
target). This script only builds the job configuration from environment variables, runs
`java -jar`, streams the jar's log, and reports success or failure. Standard library only.

Secrets (the DEK-transport private key, a STATIC token, an Azure client secret) are passed
to the jar as environment variables of the child process -- never written to disk.

Required:
  SVC_BASE_URL              host of hsm-bulk-service (or hsm-core-service), no API prefix
  SVC_APP_ID                calling app_id (needs dek_issue to encrypt, dek_unwrap to decrypt)
  CLIENT_PRIVATE_KEY_FILE   path to the app's DEK-transport private key, PKCS#8 PEM
  FILE_SOURCE_ROOT          folder to read, e.g. https://<acct>.blob.core.windows.net/<container>/<folder>
  FILE_TARGET_ROOT          folder to write; must not be inside FILE_SOURCE_ROOT
  plus, by SVC_AUTH_MODE:   AZURE_AD -> SVC_AZURE_TOKEN_SCOPE;  STATIC -> SVC_TOKEN

Optional (defaults in brackets):
  BULK_CLIENT_JAR [hsm-bulk-client.jar]   JAVA_BIN [java]
  JOB_MODE [ENCRYPT]  ENCRYPT | DECRYPT
  SVC_AUTH_MODE [AZURE_AD]  AZURE_AD | STATIC
  SVC_API_V1_PREFIX [/api/sensec/hsm/v1]
  FILE_SOURCE_TYPE / FILE_TARGET_TYPE [AZURE_BLOB]  AZURE_BLOB | ADLS | LOCAL
  FILE_TYPES [all files]   comma-separated extensions, e.g. .pdf,.csv
  FILE_DEK_NAME [unset]    one named DEK for the whole job instead of one DEK per file (ENCRYPT)
  FILE_PARALLELISM [1]     files processed concurrently
  FILE_CHUNK_SIZE_BYTES [8388608]   use 1048576 if hsm-file-service will serve the files
  FILE_FORMAT_VERSION [1]  2 only once every reader of the output supports v2
  FILE_COMPRESS_BEFORE_ENCRYPT [false]
  FIPS_APPROVED_ONLY [true]   -Dorg.bouncycastle.fips.approved_only for the whole JVM

Azure storage sign-in is done by the jar: Workload Identity (AKS), else AZURE_CLIENT_ID +
AZURE_TENANT_ID + AZURE_CLIENT_SECRET, else Managed Identity, else `az login`.

Exit code: 0 when the jar exits 0 and logs hsm_bulk_client_complete, 1 otherwise.
See README.md "run_bulk_file_job.py".
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

COMPLETE_MARKER = "hsm_bulk_client_complete"


def _require(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        sys.exit(f"error: {name} is required (see this script's header or README.md)")
    return value


def _inside(child: str, parent: str) -> bool:
    child, parent = child.rstrip("/"), parent.rstrip("/")
    return child == parent or child.startswith(parent + "/")


def build_env() -> dict[str, str]:
    mode = os.environ.get("JOB_MODE", "ENCRYPT").strip().upper()
    if mode not in ("ENCRYPT", "DECRYPT"):
        sys.exit(f"error: JOB_MODE must be ENCRYPT or DECRYPT, got {mode!r}")
    auth_mode = os.environ.get("SVC_AUTH_MODE", "AZURE_AD").strip().upper()
    if auth_mode == "AZURE_AD":
        _require("SVC_AZURE_TOKEN_SCOPE")
    elif auth_mode == "STATIC":
        _require("SVC_TOKEN")
    else:
        sys.exit(f"error: SVC_AUTH_MODE must be AZURE_AD or STATIC, got {auth_mode!r}")

    source_root = _require("FILE_SOURCE_ROOT")
    target_root = _require("FILE_TARGET_ROOT")
    if _inside(target_root, source_root):
        sys.exit("error: FILE_TARGET_ROOT must not be FILE_SOURCE_ROOT or inside it "
                 "(the next run would pick up its own output)")

    key_file = Path(_require("CLIENT_PRIVATE_KEY_FILE"))
    try:
        private_key_pem = key_file.read_text()
    except OSError as e:
        sys.exit(f"error: cannot read CLIENT_PRIVATE_KEY_FILE {key_file}: {e}")
    if "-----BEGIN PRIVATE KEY-----" not in private_key_pem:
        sys.exit("error: CLIENT_PRIVATE_KEY_FILE must be a PKCS#8 PEM (-----BEGIN PRIVATE KEY-----); "
                 "convert with: openssl pkcs8 -topk8 -nocrypt -in old.pem -out new.pem")

    env = os.environ.copy()
    env.update({
        "JOB_TYPE": "FILE",
        "JOB_MODE": mode,
        "SVC_BASE_URL": _require("SVC_BASE_URL").rstrip("/"),
        "SVC_API_V1_PREFIX": os.environ.get("SVC_API_V1_PREFIX", "/api/sensec/hsm/v1"),
        "SVC_APP_ID": _require("SVC_APP_ID"),
        "SVC_AUTH_MODE": auth_mode,
        "CLIENT_PRIVATE_KEY_PEM": private_key_pem,
        "FILE_SOURCE_TYPE": os.environ.get("FILE_SOURCE_TYPE", "AZURE_BLOB").strip().upper(),
        "FILE_SOURCE_ROOT": source_root,
        "FILE_TARGET_TYPE": os.environ.get("FILE_TARGET_TYPE", "AZURE_BLOB").strip().upper(),
        "FILE_TARGET_ROOT": target_root,
        "FILE_CHUNK_SIZE_BYTES": os.environ.get("FILE_CHUNK_SIZE_BYTES", "8388608"),
        "FILE_FORMAT_VERSION": os.environ.get("FILE_FORMAT_VERSION", "1"),
        "FILE_COMPRESS_BEFORE_ENCRYPT": os.environ.get("FILE_COMPRESS_BEFORE_ENCRYPT", "false"),
    })
    return env


def job_yaml() -> str:
    """Settings that don't map onto single environment variables (lists, the named DEK).
    Values are JSON-encoded, which is valid YAML and safe for any characters."""
    file_types = [t.strip() for t in os.environ.get("FILE_TYPES", "").split(",") if t.strip()]
    lines = [
        "client:",
        "  file:",
        f"    file-types: {json.dumps(file_types)}",
        f"    parallelism: {int(os.environ.get('FILE_PARALLELISM', '1'))}",
    ]
    dek_name = os.environ.get("FILE_DEK_NAME", "").strip()
    if dek_name:
        lines.append(f"    dek-name: {json.dumps(dek_name)}")
    return "\n".join(lines) + "\n"


def main() -> int:
    env = build_env()
    jar = os.environ.get("BULK_CLIENT_JAR", "hsm-bulk-client.jar")
    if not Path(jar).is_file():
        sys.exit(f"error: jar not found: {jar} (set BULK_CLIENT_JAR)")
    yaml_text = job_yaml()

    print(f"job: {env['JOB_MODE']}  {env['FILE_SOURCE_TYPE']} {env['FILE_SOURCE_ROOT']}"
          f"  ->  {env['FILE_TARGET_TYPE']} {env['FILE_TARGET_ROOT']}")
    print(f"svc: {env['SVC_BASE_URL']}{env['SVC_API_V1_PREFIX']}  app_id={env['SVC_APP_ID']}"
          f"  auth={env['SVC_AUTH_MODE']}")
    print("job file:\n" + yaml_text, flush=True)

    with tempfile.NamedTemporaryFile("w", suffix=".yml", prefix="hsm-bulk-job-", delete=False) as f:
        f.write(yaml_text)
        job_file = f.name
    approved_only = os.environ.get("FIPS_APPROVED_ONLY", "true").strip().lower()
    cmd = [os.environ.get("JAVA_BIN", "java"),
           f"-Dorg.bouncycastle.fips.approved_only={approved_only}",
           "-Dorg.bouncycastle.native.cpu_variant=java",   # pure-Java BC-FIPS: nothing unpacked to disk
           "-jar", jar,
           f"--spring.config.additional-location=file:{job_file}"]
    completed = False
    try:
        with subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, bufsize=1) as proc:
            for line in proc.stdout:
                sys.stdout.write(line)
                if COMPLETE_MARKER in line:
                    completed = True
            returncode = proc.wait()
    finally:
        os.unlink(job_file)

    if returncode == 0 and completed:
        print("RESULT: SUCCESS")
        return 0
    print(f"RESULT: FAILED (exit code {returncode}"
          f"{'' if completed else ', no ' + COMPLETE_MARKER + ' line'})")
    return 1


if __name__ == "__main__":
    sys.exit(main())
