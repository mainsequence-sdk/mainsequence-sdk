"""Authentication compatibility for normal CLI-backed SDK processes."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from mainsequence.bootstrap import prime_runtime_env
from mainsequence.cli import config


@pytest.fixture
def bootstrap(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    for key in ("MAINSEQUENCE_ENDPOINT", "MAINSEQUENCE_ACCESS_TOKEN", "MAINSEQUENCE_REFRESH_TOKEN"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(config, "backend_url", lambda: "https://saved.example.test")
    monkeypatch.setattr(
        config, "get_tokens", lambda: {"access": "saved-access", "refresh": "saved-refresh"}
    )
    return tmp_path


def test_saved_cli_session_is_available_to_sdk_process(bootstrap):
    prime_runtime_env()
    assert os.environ["MAINSEQUENCE_ENDPOINT"] == "https://saved.example.test"
    assert os.environ["MAINSEQUENCE_ACCESS_TOKEN"] == "saved-access"
    assert os.environ["MAINSEQUENCE_REFRESH_TOKEN"] == "saved-refresh"


def test_saved_session_brings_its_user_name_with_its_tokens(bootstrap, monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_USERNAME", "")
    monkeypatch.setattr(
        config,
        "get_tokens",
        lambda: {
            "username": "ada@example.com",
            "access": "saved-access",
            "refresh": "saved-refresh",
        },
    )
    prime_runtime_env()
    assert os.environ["MAINSEQUENCE_USERNAME"] == "ada@example.com"


def test_explicit_process_credentials_are_not_given_the_saved_user_name(bootstrap, monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_USERNAME", "")
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "explicit")
    monkeypatch.setattr(
        config,
        "get_tokens",
        lambda: {"username": "ada@example.com", "access": "explicit", "refresh": ""},
    )
    prime_runtime_env()
    # Whose token the environment carries is not known.
    assert os.environ["MAINSEQUENCE_USERNAME"] == ""


def test_bootstrap_preserves_explicit_process_credentials_and_endpoint(bootstrap, monkeypatch):
    for key in ("MAINSEQUENCE_ENDPOINT", "MAINSEQUENCE_ACCESS_TOKEN", "MAINSEQUENCE_REFRESH_TOKEN"):
        monkeypatch.setenv(key, "explicit")
    prime_runtime_env()
    assert all(
        os.environ[key] == "explicit"
        for key in (
            "MAINSEQUENCE_ENDPOINT",
            "MAINSEQUENCE_ACCESS_TOKEN",
            "MAINSEQUENCE_REFRESH_TOKEN",
        )
    )


def test_checkout_endpoint_precedes_saved_backend(bootstrap):
    (bootstrap / ".env").write_text("MAINSEQUENCE_ENDPOINT=https://checkout.example.test\n")
    prime_runtime_env()
    assert os.environ["MAINSEQUENCE_ENDPOINT"] == "https://checkout.example.test"


def test_unavailable_login_store_does_not_block_unauthenticated_source_import(
    bootstrap, monkeypatch
):
    def unavailable():
        raise RuntimeError("store unavailable")

    monkeypatch.setattr(config, "get_tokens", unavailable)
    prime_runtime_env()
    assert "MAINSEQUENCE_ACCESS_TOKEN" not in os.environ


@pytest.fixture
def credential_source(bootstrap, monkeypatch):
    from mainsequence import bootstrap as bootstrap_module

    monkeypatch.setattr(bootstrap_module, "_credential_source", None)
    for key in ("MAIN_SEQUENCE_USER_TOKEN", "MAIN_SEQUENCE_REFRESH_TOKEN"):
        monkeypatch.delenv(key, raising=False)
    return bootstrap_module.credential_source


def test_bootstrap_records_credentials_taken_from_the_saved_session(credential_source):
    prime_runtime_env()
    assert credential_source() == "store"


def test_bootstrap_records_credentials_already_in_the_environment(credential_source, monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", "explicit")
    prime_runtime_env()
    assert credential_source() == "environment"


def test_bootstrap_records_that_no_credentials_were_found(credential_source, monkeypatch):
    monkeypatch.setattr(config, "get_tokens", lambda: {})
    prime_runtime_env()
    assert credential_source() is None


def test_rejected_environment_pair_is_named_instead_of_the_saved_session(monkeypatch):
    from mainsequence import bootstrap as bootstrap_module
    from mainsequence.client import utils

    monkeypatch.setattr(bootstrap_module, "_credential_source", "environment")
    monkeypatch.setattr(config, "stored_session_available", lambda backend=None: True)
    hint = utils._jwt_reauth_hint()
    assert "MAINSEQUENCE_ACCESS_TOKEN / MAINSEQUENCE_REFRESH_TOKEN" in hint
    assert "A saved CLI session exists for this backend and was not used." in hint

    monkeypatch.setattr(config, "stored_session_available", lambda backend=None: False)
    hint = utils._jwt_reauth_hint()
    assert "No saved CLI session exists for this backend." in hint
    assert "mainsequence login" in hint


def test_rejected_saved_session_keeps_the_login_hint_and_does_not_read_the_store(monkeypatch):
    from mainsequence import bootstrap as bootstrap_module
    from mainsequence.client import utils

    store_reads = []
    monkeypatch.setattr(bootstrap_module, "_credential_source", "store")
    monkeypatch.setattr(
        config, "stored_session_available", lambda backend=None: store_reads.append(1) or True
    )

    hint = utils._jwt_reauth_hint()

    assert "`mainsequence logout` and `mainsequence login`" in hint
    assert store_reads == []


def test_package_import_bootstraps_before_client_endpoint_and_provider_initialization(tmp_path):
    script = """
import sys
import types
config = types.ModuleType("mainsequence.cli.config")
config.backend_url = lambda: "https://saved.example.test"
config.get_tokens = lambda: {"access": "saved-access", "refresh": "saved-refresh"}
cli = types.ModuleType("mainsequence.cli")
cli.__path__ = []
cli.config = config
sys.modules[cli.__name__] = cli
sys.modules[config.__name__] = config
from mainsequence.client import utils
assert utils.MAINSEQUENCE_ENDPOINT == "https://saved.example.test"
assert utils.loaders.auth_headers["Authorization"] == "Bearer saved-access"
"""
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("MAINSEQUENCE_", "MAIN_SEQUENCE_"))
    }
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
