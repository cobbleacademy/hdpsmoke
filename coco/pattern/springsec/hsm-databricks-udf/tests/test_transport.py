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
