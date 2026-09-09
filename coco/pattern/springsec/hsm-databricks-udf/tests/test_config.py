"""
Config.from_json is the path Unity Catalog Python Functions actually use
(see udf.py, sql/create_functions.sql) -- dbutils/os.environ can't be
resolved inside the function body itself, confirmed against Databricks'
own docs/KB (see DEPLOYMENT.md). Config.from_env stays for local/testing use
and is covered indirectly by test_live_interop.py.
"""

import json

import pytest

from hsm_databricks_udf.config import Config, ConfigError


def test_from_json_static_mode():
    creds = json.dumps({
        "HSM_SERVICE_BASE_URL": "https://hsm.internal/api/sensec/hsm/v1/",
        "HSM_APP_ID": "databricks-udf",
        "HSM_AUTH_MODE": "STATIC",
        "HSM_PRIVATE_KEY_PEM": "PRIVATE-KEY-PEM",
        "HSM_BEARER_TOKEN": "demo-token-databricks-udf",
    })
    config = Config.from_json(creds)
    assert config.base_url == "https://hsm.internal/api/sensec/hsm/v1"  # trailing slash stripped
    assert config.auth_mode == "STATIC"
    assert config.bearer_token == "demo-token-databricks-udf"


def test_from_json_self_signed_jwt_mode_with_dedicated_signing_key():
    creds = json.dumps({
        "HSM_SERVICE_BASE_URL": "https://hsm.internal/api/sensec/hsm/v1",
        "HSM_APP_ID": "databricks-udf",
        "HSM_AUTH_MODE": "SELF_SIGNED_JWT",
        "HSM_PRIVATE_KEY_PEM": "TRANSPORT-KEY-PEM",
        "HSM_SIGNING_PRIVATE_KEY_PEM": "SIGNING-KEY-PEM",
        "HSM_SELF_SIGNED_AUDIENCE": "some-other-audience",
    })
    config = Config.from_json(creds)
    assert config.auth_mode == "SELF_SIGNED_JWT"
    assert config.signing_private_key_pem == "SIGNING-KEY-PEM"
    assert config.self_signed_audience == "some-other-audience"


def test_from_json_self_signed_jwt_falls_back_to_transport_key():
    creds = json.dumps({
        "HSM_SERVICE_BASE_URL": "https://hsm.internal/api/sensec/hsm/v1",
        "HSM_APP_ID": "databricks-udf",
        "HSM_AUTH_MODE": "SELF_SIGNED_JWT",
        "HSM_PRIVATE_KEY_PEM": "TRANSPORT-KEY-PEM",
    })
    config = Config.from_json(creds)
    assert config.signing_private_key_pem == "TRANSPORT-KEY-PEM"
    assert config.self_signed_audience == "hsm-core-service"  # default


def test_from_json_defaults_to_static_when_auth_mode_omitted():
    creds = json.dumps({
        "HSM_SERVICE_BASE_URL": "https://hsm.internal/api/sensec/hsm/v1",
        "HSM_APP_ID": "databricks-udf",
        "HSM_PRIVATE_KEY_PEM": "PRIVATE-KEY-PEM",
        "HSM_BEARER_TOKEN": "demo-token",
    })
    assert Config.from_json(creds).auth_mode == "STATIC"


def test_from_json_rejects_invalid_json():
    with pytest.raises(ConfigError, match="not valid JSON"):
        Config.from_json("{not json")


def test_from_json_rejects_non_object_json():
    with pytest.raises(ConfigError, match="JSON object"):
        Config.from_json("[1, 2, 3]")


def test_from_json_rejects_missing_required_field():
    creds = json.dumps({"HSM_APP_ID": "databricks-udf"})
    with pytest.raises(ConfigError, match="HSM_SERVICE_BASE_URL"):
        Config.from_json(creds)


def test_from_json_rejects_unknown_auth_mode():
    creds = json.dumps({
        "HSM_SERVICE_BASE_URL": "https://hsm.internal/api/sensec/hsm/v1",
        "HSM_APP_ID": "databricks-udf",
        "HSM_AUTH_MODE": "AZURE_AD",
        "HSM_PRIVATE_KEY_PEM": "PRIVATE-KEY-PEM",
    })
    with pytest.raises(ConfigError, match="AZURE_AD and MTLS aren't implemented"):
        Config.from_json(creds)


def test_from_json_rejects_static_mode_missing_bearer_token():
    creds = json.dumps({
        "HSM_SERVICE_BASE_URL": "https://hsm.internal/api/sensec/hsm/v1",
        "HSM_APP_ID": "databricks-udf",
        "HSM_AUTH_MODE": "STATIC",
        "HSM_PRIVATE_KEY_PEM": "PRIVATE-KEY-PEM",
    })
    with pytest.raises(ConfigError, match="HSM_BEARER_TOKEN"):
        Config.from_json(creds)
