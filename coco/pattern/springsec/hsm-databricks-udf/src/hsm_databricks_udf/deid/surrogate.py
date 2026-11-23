"""
The surrogate method's primitives (java/docs/DEIDENTIFICATION.md sections 4.5 and 8),
as com.hsm.client.deid.transform.SurrogateTransform:

- ``lookup_hash = HMAC(K_lookup, domain ‖ 0x00 ‖ normalize(value))``, the table's
  primary key, so rows are found by equality without storing the value in clear;
- ``value_enc = IV ‖ AES-256-GCM(K_enc, normalize(value)) ‖ tag`` with AAD
  ``hsm-deid-surrogate-v1|<scope>|<domain>``, so a surrogate can be mapped back;
- a new surrogate: ``prefix`` (``S-``) + ``length`` (10) Crockford base32 characters
  from the OS CSPRNG, in groups of 5.

Rows written by the Java job and by this one are interchangeable: the same table
and scope can serve both. Resolving surrogates against the table (find, insert the
missing ones, re-read lost races, retry collisions) is the job's work (phase 5).
"""

from __future__ import annotations

import secrets
import uuid

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .. import iv_factory
from . import _java
from .errors import DeidError
from .keys import SURROGATE_ENC_LABEL, SURROGATE_LOOKUP_LABEL, hmac_sha256
from .methods import FieldOptions, Normalize, domain_prefix
from .profile import Method

MAX_COLLISION_RETRIES = 5
DEFAULT_PREFIX = "S-"
DEFAULT_LENGTH = 10
CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_GROUP = 5
_TAG_LENGTH = 16


class SurrogateTransform:
    method = Method.SURROGATE

    def __init__(self, domain: str, normalize: Normalize, prefix: str, length: int, scope: str,
                 lookup_key: bytes, enc_key: bytes, key_edek_id: uuid.UUID | None):
        self.domain = domain
        self.normalize = normalize
        self.prefix = prefix
        self.length = length
        self.scope = scope
        self._lookup_key = lookup_key
        self._enc_key = enc_key
        self.key_edek_id = key_edek_id
        self._domain_prefix = domain_prefix(domain)

    @staticmethod
    def key_labels() -> tuple:
        """The labels ``create`` expects keys for, in order."""
        return SURROGATE_LOOKUP_LABEL, SURROGATE_ENC_LABEL

    @classmethod
    def create(cls, o: FieldOptions, domain: str | None, scope: str, keys: tuple,
               key_edek_id: uuid.UUID | None) -> "SurrogateTransform":
        normalize = Normalize.from_label(o.string("normalize", "trim"))
        prefix = o.string("prefix", DEFAULT_PREFIX)
        length = o.integer("length", DEFAULT_LENGTH, 8, 26)
        o.reject_unknown("surrogate")
        if domain is None or not _java.strip(domain):
            raise ValueError("surrogate needs a domain (e.g. member, card)")
        return cls(domain, normalize, prefix, length, scope, keys[0], keys[1], key_edek_id)

    def normalized(self, value: str) -> str:
        return self.normalize.apply(value)

    def lookup_hash(self, normalized: str) -> bytes:
        return hmac_sha256(self._lookup_key, self._domain_prefix, normalized.encode("utf-8"))

    def new_surrogate(self) -> str:
        out = [self.prefix]
        for i in range(self.length):
            if i > 0 and i % _GROUP == 0:
                out.append("-")
            out.append(CROCKFORD[secrets.randbelow(len(CROCKFORD))])
        return "".join(out)

    def _aad(self) -> bytes:
        return ("hsm-deid-surrogate-v1|" + self.scope + "|" + self.domain).encode("utf-8")

    def encrypt_value(self, normalized: str) -> bytes:
        iv = iv_factory.generate()
        return iv + AESGCM(self._enc_key).encrypt(iv, normalized.encode("utf-8"), self._aad())

    def decrypt_value(self, value_enc: bytes) -> str:
        """Maps a stored value_enc back to the normalized value. Never used during an export."""
        if len(value_enc) < iv_factory.IV_LENGTH_BYTES + _TAG_LENGTH:
            raise DeidError.bad_value("value_enc too short")
        iv, body = value_enc[:iv_factory.IV_LENGTH_BYTES], value_enc[iv_factory.IV_LENGTH_BYTES:]
        try:
            return AESGCM(self._enc_key).decrypt(iv, body, self._aad()).decode("utf-8")
        except InvalidTag:
            raise DeidError.bad_value("value_enc failed authentication (wrong key, scope or domain)") from None

    def describe(self) -> dict:
        return {"domain": self.domain, "scope": self.scope, "normalize": self.normalize.label,
                "key_edek_id": str(self.key_edek_id)}
