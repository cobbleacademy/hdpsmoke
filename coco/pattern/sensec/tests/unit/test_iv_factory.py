from app.crypto.iv_factory import generate, IV_LENGTH_BYTES


def test_iv_length():
    assert len(generate()) == IV_LENGTH_BYTES == 12


def test_iv_unique():
    ivs = {generate() for _ in range(1000)}
    assert len(ivs) == 1000   # no collisions in 1 000 draws
