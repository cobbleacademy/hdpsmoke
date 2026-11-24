"""The hsm-deid command: credentials from separate secrets or one JSON, exit status, a run through the CLI."""

import json

import pytest

from hsm_databricks_udf.deid import cli

from test_job import PLAIN, table  # noqa: F401 - the gift-card view fixture


def args(*extra):
    return cli.parser().parse_args(["--profile", "p.yml", "--output-root", "out", "--source-table", "t", *extra])


def secrets(values):
    def get(scope, key, env=None):
        return None if key is None else values[(scope, key)]
    return get


def test_credentials_from_separate_secrets():
    a = args("--secret-scope", "hsm", "--hsm-base-url", "https://hsm/api/sensec/hsm/v1", "--hsm-app-id", "deid-dbx",
             "--hsm-private-key-key", "pk", "--hsm-token-key", "tok", "--hsm-ca-cert-key", "ca")
    got = json.loads(cli.credentials_json(a, secrets({("hsm", "pk"): "PEM", ("hsm", "tok"): "T", ("hsm", "ca"): "CA"})))
    assert got == {"HSM_SERVICE_BASE_URL": "https://hsm/api/sensec/hsm/v1", "HSM_APP_ID": "deid-dbx",
                   "HSM_AUTH_MODE": "STATIC", "HSM_API_VERSION": "V1", "HSM_PRIVATE_KEY_PEM": "PEM",
                   "HSM_BEARER_TOKEN": "T", "HSM_CA_CERT_PEM": "CA"}


def test_self_signed_jwt_credentials():
    a = args("--hsm-base-url", "https://hsm/v2", "--hsm-app-id", "x", "--hsm-auth-mode", "SELF_SIGNED_JWT",
             "--hsm-api-version", "V2", "--hsm-private-key-key", "pk", "--hsm-signing-key-key", "sk")
    got = json.loads(cli.credentials_json(a, secrets({("hsm", "pk"): "PEM", ("hsm", "sk"): "SIGN"})))
    assert got["HSM_SIGNING_PRIVATE_KEY_PEM"] == "SIGN" and "HSM_BEARER_TOKEN" not in got
    assert "HSM_SELF_SIGNED_AUDIENCE" not in got, "the default audience comes from Config"


def test_jwt_audience():
    jwt = ("--hsm-base-url", "https://hsm/v2", "--hsm-app-id", "x", "--hsm-auth-mode", "SELF_SIGNED_JWT",
           "--hsm-api-version", "V2", "--hsm-private-key-key", "pk", "--hsm-signing-key-key", "sk")
    values = secrets({("hsm", "pk"): "PEM", ("hsm", "sk"): "SIGN", ("hsm", "all"): "{}"})
    got = json.loads(cli.credentials_json(args(*jwt, "--hsm-jwt-audience", "hsm-core-prod"), values))
    assert got["HSM_SELF_SIGNED_AUDIENCE"] == "hsm-core-prod"
    with pytest.raises(SystemExit, match="needs --hsm-auth-mode SELF_SIGNED_JWT"):
        cli.credentials_json(args("--hsm-base-url", "u", "--hsm-app-id", "x", "--hsm-private-key-key", "pk",
                                  "--hsm-token-key", "tok", "--hsm-jwt-audience", "a"), values)
    with pytest.raises(SystemExit, match="put HSM_SELF_SIGNED_AUDIENCE in the JSON"):
        cli.credentials_json(args("--hsm-credentials-key", "all", "--hsm-jwt-audience", "a"), values)


def test_the_audience_reaches_the_client_config():
    from hsm_databricks_udf.config import Config
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    pem = rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode()
    a = args("--hsm-base-url", "https://hsm/api/sensec/hsm/v2", "--hsm-app-id", "x", "--hsm-auth-mode",
             "SELF_SIGNED_JWT", "--hsm-api-version", "V2", "--hsm-private-key-key", "pk",
             "--hsm-jwt-audience", "hsm-core-prod")
    config = Config.from_json(cli.credentials_json(a, secrets({("hsm", "pk"): pem})))
    assert config.self_signed_audience == "hsm-core-prod" and config.api_version == "V2"


def test_one_json_secret_or_none():
    assert cli.credentials_json(args("--hsm-credentials-key", "all"), secrets({("hsm", "all"): "{}"})) == "{}"
    assert cli.credentials_json(args(), secrets({})) is None, "a plain profile needs no hsm-core"
    with pytest.raises(SystemExit, match="need --hsm-base-url"):
        cli.credentials_json(args("--hsm-app-id", "x"), secrets({}))
    with pytest.raises(SystemExit, match="needs --hsm-token-key"):
        cli.credentials_json(args("--hsm-base-url", "u", "--hsm-app-id", "x", "--hsm-private-key-key", "pk"),
                             secrets({("hsm", "pk"): "PEM"}))


def test_init_key_needs_a_deid_name(capsys):
    assert cli.run(["--init-key", "--name", "giftcard-tok"]) == 2
    assert "deid-" in capsys.readouterr().err


def test_main_exits_with_the_status():
    with pytest.raises(SystemExit) as e:
        cli.main(["--output-root", "x"])
    assert e.value.code == 2


def test_main_returns_on_success_and_raises_on_failure(monkeypatch):
    """Databricks runs the entry point in IPython, which fails the task on any SystemExit, even 0."""
    monkeypatch.setattr(cli, "run", lambda argv=None: 0)
    assert cli.main([]) is None
    for status in (1, 2):
        monkeypatch.setattr(cli, "run", lambda argv=None, s=status: s)
        with pytest.raises(SystemExit) as e:
            cli.main([])
        assert e.value.code == status


def test_a_dry_run_and_a_failed_run_through_the_cli(spark, table, tmp_path, capsys):  # noqa: F811
    profile = tmp_path / "plain.yml"
    profile.write_text(PLAIN, encoding="utf-8")
    base = ["--profile", str(profile), "--output-root", str(tmp_path / "out"), "--source-table", "giftcards",
            "--key-column", "id"]
    assert cli.run(base + ["--dry-run", "--sample", "2"]) == 0
    assert json.loads(capsys.readouterr().out)["review"]["sample_rows"] == 2
    assert cli.run(base[:4] + ["--source-table", "no_such_table"]) == 1
    assert "hsm-deid failed: " in capsys.readouterr().err
