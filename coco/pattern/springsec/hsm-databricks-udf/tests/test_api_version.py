"""HSM_API_VERSION: V1 (default) sends/reads snake_case, V2 camelCase -- same results either way."""

from __future__ import annotations

import json

import pytest

from hsm_databricks_udf.config import Config, ConfigError
from hsm_databricks_udf.svc_client import SvcClient
from hsm_databricks_udf.auth import StaticTokenProvider


def _config(**overrides) -> dict:
    base = {"HSM_SERVICE_BASE_URL": "https://core/api/sensec/hsm/v1", "HSM_APP_ID": "app",
            "HSM_AUTH_MODE": "STATIC", "HSM_PRIVATE_KEY_PEM": "unused", "HSM_BEARER_TOKEN": "t"}
    base.update(overrides)
    return base


class _Response:
    ok = True
    status_code = 200

    def __init__(self, body: dict) -> None:
        self._body = body

    def json(self) -> dict:
        return self._body


class _Session:
    """Records each request body; answers with the canned response for its path."""

    def __init__(self, responses: dict[str, dict]) -> None:
        self.responses = responses
        self.sent: list[tuple[str, dict]] = []

    def post(self, url, json=None, headers=None, timeout=None):
        self.sent.append((url, json))
        return _Response(self.responses[url.rsplit("/", 2)[-2] + "/" + url.rsplit("/", 1)[-1]])


def _client(api_version: str | None, responses: dict[str, dict]) -> tuple[SvcClient, _Session]:
    overrides = {} if api_version is None else {"HSM_API_VERSION": api_version}
    if api_version == "V2":
        overrides["HSM_SERVICE_BASE_URL"] = "https://core/api/sensec/hsm/v2"
    session = _Session(responses)
    return SvcClient(Config.from_json(json.dumps(_config(**overrides))), session=session,
                     token_provider=StaticTokenProvider("t")), session


def test_default_is_v1():
    assert Config.from_json(json.dumps(_config())).api_version == "V1"


def test_unknown_version_is_rejected():
    with pytest.raises(ConfigError, match="HSM_API_VERSION"):
        Config.from_json(json.dumps(_config(HSM_API_VERSION="V3")))


def test_v1_is_snake_case():
    client, session = _client(None, {"dek/issue": {"items": [{"key": "1", "status": "success", "edek_id": "e1",
                                                                "wrapped_dek_b64": "d2", "owner_app_id": "o", "reused": True}]}})
    result = client.issue_dek("orders", "pii")
    url, body = session.sent[0]
    assert url == "https://core/api/sensec/hsm/v1/dek/issue"
    assert body == {"items": [{"key": "1", "name": "orders", "data_classification": "pii"}]}
    assert (result.edek_id, result.wrapped_dek_b64, result.owner_app_id, result.reused) == ("e1", "d2", "o", True)


def test_v2_is_camel_case_with_the_same_results():
    client, session = _client("V2", {
        "dek/issue": {"items": [{"key": "1", "status": "success", "edekId": "e1",
                                 "wrappedDekB64": "d2", "ownerAppId": "o", "reused": True}]},
        "dek/unwrap": {"items": [{"key": "1", "status": "success", "edekId": "e1",
                                  "wrappedDekB64": "d3", "ownerAppId": "o"}]}})
    issued = client.issue_dek("orders", "pii")
    unwrapped = client.unwrap_dek("e1")
    assert session.sent[0] == ("https://core/api/sensec/hsm/v2/dek/issue",
                               {"items": [{"key": "1", "name": "orders", "dataClassification": "pii"}]})
    assert session.sent[1] == ("https://core/api/sensec/hsm/v2/dek/unwrap", {"items": [{"key": "1", "edekId": "e1"}]})
    assert (issued.edek_id, issued.wrapped_dek_b64, issued.owner_app_id, issued.reused) == ("e1", "d2", "o", True)
    assert (unwrapped.wrapped_dek_b64, unwrapped.owner_app_id) == ("d3", "o")
