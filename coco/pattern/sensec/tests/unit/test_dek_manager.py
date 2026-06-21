"""Unit tests for DEK generation, encrypt/decrypt, and memory zeroing."""

import pytest
from app.crypto import dek_manager


def test_generate_dek_length():
    dek = dek_manager.generate_dek()
    assert len(dek) == 32


def test_generate_dek_is_random():
    a = dek_manager.generate_dek()
    b = dek_manager.generate_dek()
    assert a != b


def test_encrypt_decrypt_roundtrip():
    dek = dek_manager.generate_dek()
    plaintext = b"Hello, FIPS world!"
    result = dek_manager.encrypt(plaintext, dek, "app-a")
    recovered = dek_manager.decrypt(result.ciphertext, result.tag, result.iv, dek, "app-a")
    assert recovered == plaintext


def test_different_iv_each_call():
    dek = dek_manager.generate_dek()
    r1 = dek_manager.encrypt(b"same input", dek, "app-a")
    r2 = dek_manager.encrypt(b"same input", dek, "app-a")
    assert r1.iv != r2.iv
    assert r1.ciphertext != r2.ciphertext   # IV randomises output


def test_aad_binding_rejects_wrong_app():
    from cryptography.exceptions import InvalidTag
    dek = dek_manager.generate_dek()
    result = dek_manager.encrypt(b"secret", dek, "app-a")
    with pytest.raises(InvalidTag):
        dek_manager.decrypt(result.ciphertext, result.tag, result.iv, dek, "app-b")


def test_tampered_ciphertext_rejected():
    from cryptography.exceptions import InvalidTag
    dek = dek_manager.generate_dek()
    result = dek_manager.encrypt(b"secret", dek, "app-a")
    tampered = bytes([result.ciphertext[0] ^ 0xFF]) + result.ciphertext[1:]
    with pytest.raises(InvalidTag):
        dek_manager.decrypt(tampered, result.tag, result.iv, dek, "app-a")


def test_zero_dek_clears_bytes():
    dek = dek_manager.generate_dek()
    dek_manager.zero_dek(dek)
    assert all(b == 0 for b in dek)
