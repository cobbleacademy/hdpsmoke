import base64

import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from hsm_databricks_udf import dek_manager, transport


def _generate_keypair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


def test_wrap_unwrap_round_trip():
    private_key, public_key = _generate_keypair()
    dek = bytes(dek_manager.generate_dek())

    wrapped = transport.wrap(dek, public_key)
    unwrapped = transport.unwrap(wrapped, private_key)

    assert unwrapped == dek


def test_pem_round_trip():
    private_key, public_key = _generate_keypair()
    from cryptography.hazmat.primitives import serialization

    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()

    parsed_private = transport.parse_private_key_pem(private_pem)
    parsed_public = transport.parse_public_key_pem(public_pem)

    dek = bytes(dek_manager.generate_dek())
    wrapped = transport.wrap(dek, parsed_public)
    assert transport.unwrap(wrapped, parsed_private) == dek


def test_base64_encoded_pem_is_accepted():
    """
    A secret manager's plain string field is a common place to store this --
    e.g. Databricks `secrets put-secret --string-value "$(base64 -w0 key.pem)"`
    -- deliberately supported (not a workaround) since it sidesteps every
    newline-mangling risk a raw multi-line PEM upload is exposed to.
    """
    private_key, public_key = _generate_keypair()
    from cryptography.hazmat.primitives import serialization

    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    private_b64 = base64.b64encode(private_pem).decode()
    public_b64 = base64.b64encode(public_pem).decode()

    parsed_private = transport.parse_private_key_pem(private_b64)
    parsed_public = transport.parse_public_key_pem(public_b64)

    dek = bytes(dek_manager.generate_dek())
    wrapped = transport.wrap(dek, parsed_public)
    assert transport.unwrap(wrapped, parsed_private) == dek


def test_neither_pem_nor_base64_raises_clear_error():
    with pytest.raises(ValueError, match="neither raw PEM"):
        transport.parse_private_key_pem("this is not a key in any format!!")


def test_base64_decodes_but_is_not_pem_raises_clear_error():
    not_a_pem = base64.b64encode(b"just some random bytes, not a PEM at all").decode()
    with pytest.raises(ValueError, match="does not contain a PEM"):
        transport.parse_private_key_pem(not_a_pem)


def test_unwrap_with_mismatched_key_raises_actionable_error():
    """
    The real-world failure this guards: hsm-core-service wraps a DEK against
    whatever public_key_pem is CURRENTLY registered for an app_id. If the
    private key in use doesn't correspond to that (regenerated without
    re-registering, or the transport/signing keys got swapped in a secret
    scope), OAEP decrypt fails -- confirmed live, not hypothetical. The
    bare cryptography ValueError ("Decryption failed") gives no hint why;
    this asserts the wrapped message actually explains it.
    """
    private_key, _ = _generate_keypair()
    wrong_key, _ = _generate_keypair()
    dek = bytes(dek_manager.generate_dek())
    wrapped = transport.wrap(dek, private_key.public_key())

    with pytest.raises(ValueError, match="does not match the public_key_pem currently registered"):
        transport.unwrap(wrapped, wrong_key)
