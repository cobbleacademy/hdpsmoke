#!/usr/bin/env python3
"""
Standalone, dependency-free proof tool for hsm-bulk-client's BULK File
encrypt/decrypt pipeline -- deliberately NOT part of hsm-bulk-client itself,
which stays headless/CLI-only by design (see ClientApplication's own javadoc:
"this is a batch job, not a server"). This is a separate, throwaway
verification aid, not a production feature.

What it does, on "Run proof": drives the REAL hsm-bulk-client.jar (not a
reimplementation of its crypto) through a real ENCRYPT run then a real DECRYPT
run against a real running hsm-core-service/hsm-bulk-service, using a sample
PDF sized to guarantee multiple real chunks at the default 8 MiB
chunk-size-bytes (see SAMPLE_TARGET_MB below). Compares the round-tripped
output against the original byte-for-byte (SHA-256), and serves both PDFs so
the browser can render the decrypted one directly -- visual confirmation on
top of the hash comparison, not instead of it.

No live per-chunk progress by design (an explicit simplification, not an
oversight) -- FileBulkJob only logs progress at the file level, not the
chunk level, and there was no reason to add chunk-level logging just for this
tool. This runs the real pipeline to completion, then shows the result.

The "compress-before-encrypt" checkbox sets that flag on the ENCRYPT job
only, never DECRYPT -- FileBulkJob reads a per-chunk marker byte instead, so
the decrypt side needs no matching config. Round-trips through the same real
jar either way.

Prerequisites (this script does NOT start these for you):
  - hsm-core-service and hsm-bulk-service both already running (demo mode,
    sharing one H2 file via AUTO_SERVER=TRUE -- hsm-bulk-service has no seed
    data of its own, it relies on hsm-core-service's DemoSeedInitializer
    having already created the payments-svc app_registrations row).
  - payments-svc provisioned with dek_issue/dek_unwrap scopes and a
    public_key_pem matching demo-private-key.pem alongside this script (see
    this directory's README for the exact SQL used to provision it).
  - hsm-bulk-client/target/hsm-bulk-client.jar already built
    (mvn -pl hsm-bulk-client package -DskipTests).

Usage:
  python3 server.py [--port 8000]
  open http://localhost:8000

Pointing at services that aren't the local demo defaults (e.g. a remote
deployment): override via env vars before launching, all optional --
  PROOF_UI_SVC_BASE_URL     default http://localhost:3006
  PROOF_UI_API_V1_PREFIX    default /api/sensec/hsm/v1 -- must match that
                            deployment's hsm.service.api-v1-prefix, not
                            necessarily the local demo's
  PROOF_UI_APP_ID           default payments-svc
  PROOF_UI_TOKEN            default demo-token-payments-svc
  JAVA_BIN                  default: resolved from PATH
  SVC_INSECURE_TLS=true     TESTING ONLY. Disables TLS certificate/hostname
                            verification for the jar's connection to SVC
                            (SvcClient.java) -- for a self-signed remote SVC
                            you don't want to import into the JVM trust
                            store. Never set this against anything but a
                            deployment you control and trust; prefer
                            importing the real certificate instead.

Proving the encrypted intermediate is really read back in chunks from Azure
storage (not just local disk): pick "ADLS Gen2" or "Azure Blob Storage" in
the UI's store selector -- which one depends on your actual storage
account, they are NOT interchangeable:
  - ADLS Gen2 requires Hierarchical Namespace (HNS) enabled on the account.
    Root: abfss://<container>@<account>.dfs.core.windows.net/<path>
    (AdlsFileStore).
  - Azure Blob Storage needs no HNS, and has no known conflict with the
    account-level "soft delete for blobs" feature the way ADLS Gen2's Data
    Lake REST API does (a real, Microsoft-documented incompatibility) --
    use this one if HNS is off, or if blob soft delete is on.
    Root: https://<account>.blob.core.windows.net/<container>/<path>
    (AzureBlobFileStore).
Set PROOF_UI_AZURE_ROOT to the appropriate URI for whichever you pick, then
select it in the UI. The ENCRYPT job writes its chunked output there
instead of local disk, and the DECRYPT job reads it back from there -- this
script itself never touches Azure storage directly, no Azure SDK dependency
added, the real jar does all of it via the matching FileStore class, same
as everything else this tool proves. Credentials resolve the normal way
(WorkloadIdentityCredential -> ManagedIdentityCredential ->
DefaultAzureCredential, i.e. `az login` works from a dev machine) unless
PROOF_UI_AZURE_ACCOUNT_KEY is also set, which forces shared-key auth
instead -- see ClientProperties.File.StoreRef's javadoc for why that's a
deliberate local-testing-only escape hatch, never for a real deployment.
  PROOF_UI_AZURE_ROOT          unset by default -- required to use either Azure store
  PROOF_UI_AZURE_ACCOUNT_KEY   unset by default -- local/dev testing only
"""
import argparse
import hashlib
import http.server
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
MODULE_DIR = SCRIPT_DIR.parent
JAR_PATH = MODULE_DIR / "target" / "hsm-bulk-client.jar"
SAMPLE_PDF = SCRIPT_DIR / "sample.pdf"
PRIVATE_KEY_PATH = SCRIPT_DIR / "demo-private-key.pem"
WORK_DIR = SCRIPT_DIR / "work"

SVC_BASE_URL = os.environ.get("PROOF_UI_SVC_BASE_URL", "http://localhost:3006")
API_V1_PREFIX = os.environ.get("PROOF_UI_API_V1_PREFIX", "/api/sensec/hsm/v1")
APP_ID = os.environ.get("PROOF_UI_APP_ID", "payments-svc")
TOKEN = os.environ.get("PROOF_UI_TOKEN", "demo-token-payments-svc")
AZURE_ROOT = os.environ.get("PROOF_UI_AZURE_ROOT", "")
AZURE_ACCOUNT_KEY = os.environ.get("PROOF_UI_AZURE_ACCOUNT_KEY", "")
CHUNK_SIZE_BYTES = 8388608  # 8 MiB, matches FileBulkJob's own default
SAMPLE_TARGET_MB = 80  # minimum -- sample.pdf is regenerated if smaller than this

# Resolved via PATH by default (shutil.which), not a hardcoded install path --
# this script runs on whatever machine hosts it, not just the one it was
# built on. Override with JAVA_BIN if the java you want isn't on PATH.
JAVA_BIN = os.environ.get("JAVA_BIN") or shutil.which("java") or "java"


def ensure_sample_pdf():
    """Generates sample.pdf if missing or under the target size -- random-noise
    image saved as a single-page PDF via Pillow, sized so JPEG's mild
    compression on high-entropy data still lands comfortably over the target.
    Regenerable, not committed as a static blob for its own sake."""
    if SAMPLE_PDF.exists() and SAMPLE_PDF.stat().st_size >= SAMPLE_TARGET_MB * 1024 * 1024:
        return
    from PIL import Image
    print(f"Generating a >={SAMPLE_TARGET_MB}MB sample PDF (one-time)...", file=sys.stderr)
    w = h = 6900
    raw = os.urandom(w * h * 3)
    img = Image.frombytes("RGB", (w, h), raw)
    img.save(str(SAMPLE_PDF), "PDF", resolution=150.0, quality=100)
    size_mb = SAMPLE_PDF.stat().st_size / (1024 * 1024)
    print(f"Wrote {SAMPLE_PDF} ({size_mb:.1f} MB)", file=sys.stderr)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _store_yaml(store_type: str, root, account_key: str = "") -> str:
    """Renders one source:/target: block. root is a local Path for LOCAL, an
    abfss://... URI for ADLS, or an https://...blob.core.windows.net/... URI
    for AZURE_BLOB -- ClientProperties.File.StoreRef takes the same 3 fields
    regardless of which. account-key is only ever emitted for the two Azure
    types, and only when explicitly set -- see StoreRef's javadoc: its mere
    presence in the config (not just being true/false) is what activates
    shared-key auth over the normal WorkloadIdentity chain, so it must never
    be emitted for LOCAL or when unset."""
    lines = [f"      type: {store_type}", f"      root: {root}"]
    if store_type in ("ADLS", "AZURE_BLOB") and account_key:
        lines.append(f'      account-key: "{account_key}"')
    return "\n".join(lines)


def write_job_yaml(path: Path, mode: str, source: tuple, target: tuple, compress: bool = False):
    """source/target are (store_type, root) tuples, e.g. ("LOCAL", some_path)
    or ("ADLS", "abfss://...") -- see _store_yaml. Keeping this generic
    rather than always-local is what lets the ENCRYPT job write its output
    straight to a real ADLS container and the DECRYPT job read it back from
    there, proving AdlsFileStore's real chunked I/O, not just LocalFileStore's."""
    private_key_pem = PRIVATE_KEY_PATH.read_text()
    indented_key = "\n".join("      " + line for line in private_key_pem.splitlines())
    # compress-before-encrypt is an ENCRYPT-side-only knob -- decrypt never
    # sets it, every chunk carries its own compressed/raw marker byte inside
    # the AES-GCM-authenticated payload (see FileBulkJob.java's class
    # javadoc), so there's nothing for the decrypt job to be told.
    compress_line = "    compress-before-encrypt: true\n" if (compress and mode == "ENCRYPT") else ""
    source_block = _store_yaml(*source)
    target_block = _store_yaml(*target)
    path.write_text(f"""\
client:
  job:
    type: FILE
    mode: {mode}
  svc:
    base-url: {SVC_BASE_URL}
    api-v1-prefix: {API_V1_PREFIX}
    app-id: {APP_ID}
    auth-mode: STATIC
    token: "{TOKEN}"
    dek-batch-max-items: 100
    private-key-pem: |
{indented_key}
  file:
    source:
{source_block}
    target:
{target_block}
    file-types: []   # match all -- the source dir here only ever holds the one chosen file anyway
    chunk-size-bytes: {CHUNK_SIZE_BYTES}
    files-per-batch: 10
    parallelism: 1
{compress_line}""")


_EXCEPTION_START = re.compile(r'^(Exception in thread|Caused by:|[\w.$]+(Exception|Error):)')


def _relevant_log_excerpt(log: str, max_lines: int = 80) -> str:
    """A blind last-N-lines tail regularly lands entirely inside Spring
    Boot's generic runner/shutdown unwind (ThrowingConsumer, SpringApplication
    internals, JarLauncher...), cutting off the one line that actually says
    what broke -- found live: a real failure's true `Caused by:` sat well
    above where a 25-line tail started. Instead, find where the first real
    exception line begins (skipping "at ..." stack frames, which can contain
    "Exception" in a frame's own class name) and take from there."""
    lines = log.splitlines()
    start = next(
        (i for i, ln in enumerate(lines)
         if _EXCEPTION_START.match(ln.strip()) and not ln.strip().startswith("at ")),
        max(0, len(lines) - 25),
    )
    return "\n".join(lines[start:start + max_lines])


def run_bulk_client(job_yaml: Path) -> dict:
    start = time.time()
    proc = subprocess.run(
        [JAVA_BIN, "-jar", str(JAR_PATH),
         f"--spring.config.additional-location=file:{job_yaml}"],
        capture_output=True, text=True, timeout=180,
    )
    elapsed = time.time() - start
    log = proc.stdout + proc.stderr
    return {
        "exit_code": proc.returncode,
        "elapsed_seconds": round(elapsed, 2),
        "log_tail": _relevant_log_excerpt(log),
        "success": proc.returncode == 0 and "hsm_bulk_client_complete" in log,
    }


def run_proof(source_file: Path = None, display_name: str = None, compress: bool = False,
              store_type: str = "LOCAL") -> dict:
    """source_file/display_name let a caller point this at an arbitrary local
    file (via the UI's path field) instead of the built-in sample.pdf --
    everything else about the flow (encrypt -> decrypt -> hash-compare -> serve
    for the browser) is identical either way, this is the only thing that
    varies. Hash comparison works for any file type; the browser preview
    (<embed type="application/pdf">) specifically assumes PDF, same as the
    whole rest of this conversation's context -- a non-PDF file would still
    get correctly verified, it just wouldn't render in the preview panes.

    store_type "ADLS" or "AZURE_BLOB" routes the encrypted intermediate
    through that real Azure store instead of a local dir: ENCRYPT writes to
    PROOF_UI_AZURE_ROOT, DECRYPT reads back from that same location --
    proving AdlsFileStore's/AzureBlobFileStore's real chunked read/write,
    not just LocalFileStore's. Which of the two matches your actual storage
    account is NOT a preference -- ADLS needs Hierarchical Namespace
    enabled; AZURE_BLOB is for accounts without it, or with "soft delete for
    blobs" on (a real, documented conflict with ADLS Gen2's Data Lake REST
    API) -- see AzureBlobFileStore.java's class javadoc. The plaintext
    source and the final decrypted output stay local either way, since
    those are what get hashed/served for the browser regardless of where
    the encrypted bytes lived in between."""
    if not JAR_PATH.exists():
        return {"ok": False, "error": f"{JAR_PATH} not found -- build it first: "
                                       "mvn -pl hsm-bulk-client package -DskipTests"}
    if store_type != "LOCAL" and not AZURE_ROOT:
        return {"ok": False, "error": f"{store_type} requested but PROOF_UI_AZURE_ROOT is not set -- "
                                       "see this script's module docstring"}
    if source_file is None:
        ensure_sample_pdf()
        source_file = SAMPLE_PDF
        display_name = "sample.pdf"

    if WORK_DIR.exists():
        shutil.rmtree(WORK_DIR)
    encrypt_source = WORK_DIR / "encrypt-source"
    encrypt_target = WORK_DIR / "encrypt-target"
    decrypt_target = WORK_DIR / "decrypt-target"
    for d in (encrypt_source, encrypt_target, decrypt_target):
        d.mkdir(parents=True)
    # Always copied in under a fixed working name, not the original filename --
    # this is purely FileBulkJob's own working file, keeping the rest of this
    # function's logic (encrypt_target / WORKING_NAME etc.) independent of
    # whatever the uploaded file was actually called. display_name (shown to
    # the user, and the served download's filename) is tracked separately.
    working_name = "input.pdf"
    shutil.copy(source_file, encrypt_source / working_name)

    original_size = source_file.stat().st_size
    expected_chunks = -(-original_size // CHUNK_SIZE_BYTES)  # ceil div

    # The encrypted intermediate's store: a real ADLS/Blob container (per the
    # UI's store-type selector) or the usual local dir. Either way both the
    # ENCRYPT job's target and the DECRYPT job's source point at the exact
    # same location -- DECRYPT reads back whatever ENCRYPT actually wrote.
    if store_type == "LOCAL":
        encrypted_store = ("LOCAL", encrypt_target)
    elif AZURE_ACCOUNT_KEY:
        encrypted_store = (store_type, AZURE_ROOT, AZURE_ACCOUNT_KEY)
    else:
        encrypted_store = (store_type, AZURE_ROOT)

    encrypt_yaml = WORK_DIR / "encrypt-job.yml"
    write_job_yaml(encrypt_yaml, "ENCRYPT", ("LOCAL", encrypt_source), encrypted_store, compress=compress)
    encrypt_result = run_bulk_client(encrypt_yaml)

    encrypted_file = encrypt_target / working_name  # only meaningful when store_type == LOCAL
    if not encrypt_result["success"] or (store_type == "LOCAL" and not encrypted_file.exists()):
        return {"ok": False, "error": "ENCRYPT run failed", "encrypt": encrypt_result}

    decrypt_yaml = WORK_DIR / "decrypt-job.yml"
    write_job_yaml(decrypt_yaml, "DECRYPT", encrypted_store, ("LOCAL", decrypt_target))
    decrypt_result = run_bulk_client(decrypt_yaml)

    decrypted_file = decrypt_target / working_name
    if not decrypt_result["success"] or not decrypted_file.exists():
        return {"ok": False, "error": "DECRYPT run failed",
                "encrypt": encrypt_result, "decrypt": decrypt_result}

    original_sha256 = sha256_of(source_file)
    decrypted_sha256 = sha256_of(decrypted_file)
    decrypted_size = decrypted_file.stat().st_size
    # Not locally readable when the intermediate lives in Azure storage --
    # this script deliberately adds no Azure SDK dependency just to report
    # this one number; the real jar's own log line (hsm_bulk_client_complete)
    # is what actually proves the ADLS/Blob write/read happened.
    encrypted_size = encrypted_file.stat().st_size if store_type == "LOCAL" else None

    # Kept for the /original.pdf and /decrypted.pdf endpoints to serve from,
    # since source_file itself may be a temp upload path that won't outlive
    # this request.
    shutil.copy(source_file, WORK_DIR / "original.pdf")
    shutil.copy(decrypted_file, WORK_DIR / "served-decrypted.pdf")

    return {
        "ok": True,
        "display_name": display_name,
        "compress": compress,
        "store_type": store_type,
        "match": original_sha256 == decrypted_sha256,
        "expected_chunks": expected_chunks,
        "chunk_size_bytes": CHUNK_SIZE_BYTES,
        "original_size": original_size,
        "encrypted_size": encrypted_size,
        "decrypted_size": decrypted_size,
        "original_sha256": original_sha256,
        "decrypted_sha256": decrypted_sha256,
        "encrypt": encrypt_result,
        "decrypt": decrypt_result,
    }


INDEX_HTML = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>hsm-bulk-client decrypt proof</title>
<style>
  body { font-family: -apple-system, sans-serif; max-width: 900px; margin: 2rem auto; padding: 0 1rem; }
  button { font-size: 1rem; padding: 0.6rem 1.2rem; cursor: pointer; }
  button:disabled { opacity: 0.5; cursor: default; }
  table { border-collapse: collapse; margin-top: 1rem; width: 100%; }
  td, th { border: 1px solid #ccc; padding: 0.4rem 0.6rem; text-align: left; font-size: 0.9rem; }
  .pass { color: #0a7d1e; font-weight: bold; }
  .fail { color: #c62828; font-weight: bold; }
  .hash { font-family: monospace; font-size: 0.75rem; word-break: break-all; }
  pre { background: #f5f5f5; padding: 0.75rem; overflow-x: auto; font-size: 0.75rem; }
  .previews { display: flex; gap: 1rem; margin-top: 1.5rem; }
  .previews > div { flex: 1; }
  embed { width: 100%; height: 500px; border: 1px solid #ccc; }
  #status { margin-top: 1rem; }
  #pathInput { width: 30rem; max-width: 70%; padding: 0.4rem; font-size: 0.95rem; }
  .controls { display: flex; gap: 0.5rem; align-items: center; flex-wrap: wrap; }
</style>
</head>
<body>
<h1>hsm-bulk-client BULK File decrypt proof</h1>
<p>Runs a real ENCRYPT then DECRYPT of a file through the actual
<code>hsm-bulk-client.jar</code>, against a real running hsm-core-service /
hsm-bulk-service. Compares the round-tripped output against the original
byte-for-byte (SHA-256), then renders both PDFs below.</p>
<div class="controls">
  <input type="text" id="pathInput" placeholder="/absolute/path/to/file.pdf -- leave blank for the built-in >=80MB sample">
  <label><input type="checkbox" id="compressInput"> compress-before-encrypt</label>
  <label title="PROOF_UI_AZURE_ROOT must be set -- see this script's docstring">encrypted intermediate:
    <select id="storeTypeInput">
      <option value="LOCAL" selected>local disk</option>
      <option value="ADLS">ADLS Gen2 (HNS required)</option>
      <option value="AZURE_BLOB">Azure Blob Storage</option>
    </select>
  </label>
  <button id="runBtn" onclick="runProof()">Run proof</button>
</div>
<span id="status"></span>
<div id="result"></div>
<script>
async function runProof() {
  const btn = document.getElementById('runBtn');
  const status = document.getElementById('status');
  const result = document.getElementById('result');
  const path = document.getElementById('pathInput').value.trim();
  const compress = document.getElementById('compressInput').checked;
  const storeType = document.getElementById('storeTypeInput').value;
  btn.disabled = true;
  status.textContent = ' Running encrypt + decrypt through the real pipeline...';
  result.innerHTML = '';
  try {
    const res = await fetch('/api/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path: path, compress: compress, store_type: storeType }),
    });
    const data = await res.json();
    status.textContent = '';
    if (!data.ok) {
      result.innerHTML = '<p class="fail">FAILED: ' + (data.error || 'unknown error') + '</p>' +
        '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
      return;
    }
    const verdict = data.match
      ? '<span class="pass">MATCH -- decrypted output is byte-for-byte identical to the original</span>'
      : '<span class="fail">MISMATCH -- decrypted output differs from the original</span>';
    const encryptedSizeCell = data.encrypted_size === null
      ? `n/a -- lives in ${data.store_type}, not read back locally`
      : data.encrypted_size.toLocaleString();
    const storeSuffix = data.store_type !== 'LOCAL' ? ` (via ${data.store_type})` : '';
    result.innerHTML = `
      <h2>${data.display_name}${data.compress ? ' (compress-before-encrypt)' : ''}${storeSuffix}: ${verdict}</h2>
      <table>
        <tr><th></th><th>Size (bytes)</th><th>SHA-256</th></tr>
        <tr><td>Original</td><td>${data.original_size.toLocaleString()}</td><td class="hash">${data.original_sha256}</td></tr>
        <tr><td>Encrypted (intermediate)${storeSuffix}</td><td>${encryptedSizeCell}</td><td class="hash">framing overhead expected, not compared</td></tr>
        <tr><td>Decrypted (round-tripped)</td><td>${data.decrypted_size.toLocaleString()}</td><td class="hash">${data.decrypted_sha256}</td></tr>
      </table>
      <p>Chunked into ${data.expected_chunks} chunks at ${(data.chunk_size_bytes / 1024 / 1024).toFixed(0)} MiB each.
         Encrypt: ${data.encrypt.elapsed_seconds}s. Decrypt: ${data.decrypt.elapsed_seconds}s.</p>
      <details><summary>Encrypt log (tail)</summary><pre>${data.encrypt.log_tail}</pre></details>
      <details><summary>Decrypt log (tail)</summary><pre>${data.decrypt.log_tail}</pre></details>
      <div class="previews">
        <div><h3>Original</h3><embed src="/original.pdf?t=${Date.now()}" type="application/pdf"></div>
        <div><h3>Decrypted (round-tripped)</h3><embed src="/decrypted.pdf?t=${Date.now()}" type="application/pdf"></div>
      </div>
    `;
  } catch (e) {
    status.textContent = '';
    result.innerHTML = '<p class="fail">Request failed: ' + e + '</p>';
  } finally {
    btn.disabled = false;
  }
}
</script>
</body>
</html>
"""


class Handler(http.server.BaseHTTPRequestHandler):
    def _send_file(self, path: Path, content_type: str):
        if not path.exists():
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(path.stat().st_size))
        # Every run overwrites these files under the same name (see
        # run_proof) -- without this, a browser or embedded PDF viewer that
        # caches by path (ignoring the frontend's own ?t= query-string
        # busting) can silently keep serving a previous run's PDF instead of
        # this one, or the current-run request can 404 against a stale cache
        # entry. Found live: the "Original" pane showed an earlier run's PDF.
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        with open(path, "rb") as f:
            shutil.copyfileobj(f, self.wfile)

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            body = INDEX_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path.startswith("/original.pdf"):
            self._send_file(WORK_DIR / "original.pdf", "application/pdf")
        elif self.path.startswith("/decrypted.pdf"):
            self._send_file(WORK_DIR / "served-decrypted.pdf", "application/pdf")
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path == "/api/run":
            try:
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length) if length else b"{}"
                payload = json.loads(raw or b"{}")
                path_str = (payload.get("path") or "").strip()
                compress = bool(payload.get("compress", False))
                store_type = payload.get("store_type") or "LOCAL"
                if store_type not in ("LOCAL", "ADLS", "AZURE_BLOB"):
                    raise ValueError(f"invalid store_type: {store_type!r}")
                if path_str:
                    source_file = Path(path_str).expanduser()
                    if not source_file.is_file():
                        raise FileNotFoundError(f"No such file: {source_file}")
                    result = run_proof(source_file=source_file, display_name=source_file.name,
                                        compress=compress, store_type=store_type)
                else:
                    result = run_proof(compress=compress, store_type=store_type)
            except subprocess.TimeoutExpired:
                result = {"ok": False, "error": "hsm-bulk-client run timed out (>180s)"}
            except Exception as e:
                result = {"ok": False, "error": f"{type(e).__name__}: {e}"}
            body = json.dumps(result).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    if not shutil.which(JAVA_BIN) and not Path(JAVA_BIN).is_file():
        print(f"error: java not found at '{JAVA_BIN}' -- set JAVA_BIN to your "
              f"java executable's full path, or put java on PATH", file=sys.stderr)
        sys.exit(1)

    ensure_sample_pdf()
    # Bind 127.0.0.1 explicitly, not "localhost" -- some clients resolve
    # "localhost" to ::1 first and hang/timeout before falling back to IPv4,
    # which is all this server actually binds (found live while testing this
    # script: curl http://localhost:8000 timed out, curl http://127.0.0.1:8000
    # worked instantly, same server, same port).
    server = http.server.ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"Serving on http://127.0.0.1:{args.port}", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
