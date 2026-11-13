"""
HSM_CA_CERT_PEM: trusting an internal/corporate CA for hsm-core-service's TLS
certificate. A real HTTPS server on 127.0.0.1 presents a certificate issued by a
throwaway test CA; the client must reject it by default (the
CERTIFICATE_VERIFY_FAILED seen from Databricks), accept it once that CA is in
HSM_CA_CERT_PEM, and still reject it when a different CA is given. Plus config
validation: base64 accepted, private keys and non-certificates refused.
"""

from __future__ import annotations

import base64
import datetime
import ipaddress
import json
import ssl
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
import requests
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from hsm_databricks_udf.config import Config, ConfigError
from hsm_databricks_udf.svc_client import SvcClient

ISSUE_RESPONSE = {"items": [{
    "key": "1", "status": "success", "edek_id": "e-1", "wrapped_dek_b64": "d3JhcHBlZA==",
    "owner_app_id": "databricks-udf", "reused": False,
}]}


def _name(cn: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])


def _make_ca(cn: str):
    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder().subject_name(_name(cn)).issuer_name(_name(cn))
        .public_key(key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5)).not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.KeyUsage(digital_signature=True, key_cert_sign=True, crl_sign=True,
                                     content_commitment=False, key_encipherment=False, data_encipherment=False,
                                     key_agreement=False, encipher_only=False, decipher_only=False), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .sign(key, hashes.SHA256())
    )
    return key, cert


LOCAL_SANS = [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]


def _make_server_cert(ca_key, ca_cert, sans=None):
    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder().subject_name(_name("localhost")).issuer_name(ca_cert.subject)
        .public_key(key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5)).not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName(sans or LOCAL_SANS), critical=False)
        .add_extension(x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    return key, cert


def _pem(cert) -> str:
    return cert.public_bytes(serialization.Encoding.PEM).decode()


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        body = json.dumps(ISSUE_RESPONSE).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def _start_server(tmp_dir, sans=None):
    """HTTPS server on 127.0.0.1 with a cert from a fresh private test CA. Returns (httpd, base_url, ca_pem)."""
    ca_key, ca_cert = _make_ca("HSM Test Internal Root CA")
    srv_key, srv_cert = _make_server_cert(ca_key, ca_cert, sans)
    cert_file, key_file = tmp_dir / "server.pem", tmp_dir / "server.key"
    cert_file.write_text(_pem(srv_cert))
    key_file.write_bytes(srv_key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                                serialization.NoEncryption()))
    httpd = HTTPServer(("127.0.0.1", 0), _Handler)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert_file, key_file)
    httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, f"https://127.0.0.1:{httpd.server_address[1]}/api/sensec/hsm/v1", _pem(ca_cert)


def _stop(httpd):
    httpd.shutdown()
    httpd.server_close()


@pytest.fixture(scope="module")
def tls_server(tmp_path_factory):
    """Yields (base_url, ca_pem) for a server whose cert names localhost/127.0.0.1."""
    httpd, base_url, ca_pem = _start_server(tmp_path_factory.mktemp("tls"))
    try:
        yield base_url, ca_pem
    finally:
        _stop(httpd)


@pytest.fixture(scope="module")
def misnamed_tls_server(tmp_path_factory):
    """Same, but the cert names only hsm.other.example -- reached via 127.0.0.1 it is a hostname mismatch."""
    httpd, base_url, ca_pem = _start_server(tmp_path_factory.mktemp("tls-misnamed"),
                                            [x509.DNSName("hsm.other.example")])
    try:
        yield base_url, ca_pem
    finally:
        _stop(httpd)


def _config(base_url: str, ca_pem: str | None = None) -> Config:
    creds = {
        "HSM_SERVICE_BASE_URL": base_url, "HSM_APP_ID": "databricks-udf", "HSM_AUTH_MODE": "STATIC",
        "HSM_PRIVATE_KEY_PEM": "unused-here", "HSM_BEARER_TOKEN": "t",
    }
    if ca_pem is not None:
        creds["HSM_CA_CERT_PEM"] = ca_pem
    return Config.from_json(json.dumps(creds))


def test_private_ca_is_rejected_without_the_bundle(tls_server):
    base_url, _ = tls_server
    with pytest.raises(requests.exceptions.SSLError, match="CERTIFICATE_VERIFY_FAILED"):
        SvcClient(_config(base_url)).issue_dek("ca.test")


def test_private_ca_is_trusted_with_the_bundle(tls_server):
    base_url, ca_pem = tls_server
    result = SvcClient(_config(base_url, ca_pem)).issue_dek("ca.test")
    assert result.edek_id == "e-1"


def test_bundle_given_as_base64_of_pem_works(tls_server):
    base_url, ca_pem = tls_server
    b64 = base64.b64encode(ca_pem.encode()).decode()
    assert SvcClient(_config(base_url, b64)).issue_dek("ca.test").edek_id == "e-1"


def test_bundle_with_extra_certificates_still_works(tls_server):
    base_url, ca_pem = tls_server
    _, other_ca = _make_ca("Some Other Intermediate")
    assert SvcClient(_config(base_url, _pem(other_ca) + ca_pem)).issue_dek("ca.test").edek_id == "e-1"


def test_a_different_ca_is_still_rejected(tls_server):
    base_url, _ = tls_server
    _, wrong_ca = _make_ca("Wrong CA")
    with pytest.raises(requests.exceptions.SSLError, match="CERTIFICATE_VERIFY_FAILED"):
        SvcClient(_config(base_url, _pem(wrong_ca))).issue_dek("ca.test")


def test_hostname_is_still_checked(misnamed_tls_server):
    """The right CA does not excuse a certificate issued for a different host name."""
    base_url, ca_pem = misnamed_tls_server
    with pytest.raises(requests.exceptions.SSLError, match="mismatch|doesn't match|not valid for"):
        SvcClient(_config(base_url, ca_pem)).issue_dek("ca.test")


# --- config validation ---------------------------------------------------------

def test_absent_bundle_means_public_cas_only():
    assert _config("https://hsm.internal/api/sensec/hsm/v1").ca_cert_pem is None


def test_private_key_is_refused():
    key = ec.generate_private_key(ec.SECP256R1())
    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                            serialization.NoEncryption()).decode()
    with pytest.raises(ConfigError, match="private key"):
        _config("https://hsm.internal/api/sensec/hsm/v1", pem)


def test_non_certificate_pem_is_refused():
    key = ec.generate_private_key(ec.SECP256R1())
    pub = key.public_key().public_bytes(serialization.Encoding.PEM,
                                        serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    with pytest.raises(ConfigError, match="BEGIN CERTIFICATE"):
        _config("https://hsm.internal/api/sensec/hsm/v1", pub)


def test_garbage_is_refused():
    with pytest.raises(ConfigError, match="neither PEM nor base64"):
        _config("https://hsm.internal/api/sensec/hsm/v1", "not a cert!")


def test_corrupt_certificate_is_refused():
    bad = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"
    with pytest.raises(ConfigError, match="does not parse"):
        _config("https://hsm.internal/api/sensec/hsm/v1", bad)
