"""Authentication compatibility for normal CLI-backed SDK processes."""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

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


def _jwt(expires_at: int) -> str:
    def part(data: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip("=")

    return f"{part({'alg': 'none'})}.{part({'exp': expires_at, 'token_type': 'refresh'})}.signature"


@pytest.fixture
def refused(monkeypatch, tmp_path):
    """A process whose environment pair the backend refused, on a machine with a saved session."""
    from mainsequence import bootstrap as bootstrap_module
    from mainsequence.client import utils

    now = int(time.time())
    expired_at, saved_until = now - 13 * 3600, now + 4 * 86400
    refresh = _jwt(expired_at)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(utils, "AUTH_ENDPOINT", "http://127.0.0.1:8000")
    monkeypatch.setattr(bootstrap_module, "_credential_source", "environment")
    monkeypatch.setattr(
        config, "get_persistent_config", lambda: {"backend_url": "http://127.0.0.1:8000"}
    )
    monkeypatch.setattr(
        config,
        "saved_session_summary",
        lambda backend=None: {
            "usable": backend == "http://127.0.0.1:8000",
            "username": "dev@example.test",
            "expires_at": saved_until,
        },
    )
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "copied-access")
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", refresh)
    return SimpleNamespace(
        utils=utils, directory=tmp_path, refresh=refresh, expired_at=expired_at, saved_until=saved_until
    )


def test_refused_pair_from_a_dotenv_names_the_file_the_expiry_and_the_repair(refused):
    utils = refused.utils
    env_file = refused.directory / ".env"
    env_file.write_text(
        "MAINSEQUENCE_ENDPOINT=http://127.0.0.1:8000\n"
        f"export MAINSEQUENCE_REFRESH_TOKEN='{refused.refresh}' # exported by a tool\n"
    )

    hint = utils._jwt_reauth_hint()

    assert f" The refresh token expired on {utils._format_utc(refused.expired_at)}." in hint
    assert f" These credentials come from the token lines of {env_file}, not from your saved session." in hint
    assert (
        " Your saved session for http://127.0.0.1:8000 (dev@example.test) is usable, valid until "
        f"{utils._format_utc(refused.saved_until)}: run `mainsequence refresh-token` in "
        f"{refused.directory} to remove those lines, and the next start uses it."
    ) in hint
    assert refused.refresh not in hint
    assert "copied-access" not in hint


def test_refused_pair_set_by_the_starting_program_says_to_start_without_it(refused):
    (refused.directory / ".env").write_text("MAINSEQUENCE_REFRESH_TOKEN=another-value\n")

    hint = refused.utils._jwt_reauth_hint()

    assert "set by the program that started it, not taken from your saved session." in hint
    assert ": start the process without those two variables, and the next start uses it." in hint


def test_refused_pair_without_a_usable_saved_session_says_to_sign_in_first(refused, monkeypatch):
    monkeypatch.setattr(
        config,
        "saved_session_summary",
        lambda backend=None: {"usable": False, "username": "", "expires_at": None},
    )

    assert (
        " There is no usable saved session for http://127.0.0.1:8000. Sign in with "
        "`mainsequence login`, then start the process without those two variables."
    ) in refused.utils._jwt_reauth_hint()


def test_signing_in_to_another_backend_names_it_and_its_base_folder(refused, monkeypatch):
    monkeypatch.setattr(
        config, "get_persistent_config", lambda: {"backend_url": "https://api.main-sequence.app/"}
    )
    monkeypatch.setattr(
        config,
        "saved_session_summary",
        lambda backend=None: {"usable": False, "username": "", "expires_at": None},
    )

    assert (
        "Sign in with `mainsequence login http://127.0.0.1:8000 <base folder>`"
        in refused.utils._jwt_reauth_hint()
    )


def test_refused_token_that_has_not_expired_is_called_revoked(refused, monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_REFRESH_TOKEN", _jwt(int(time.time()) + 3600))

    assert (
        " The refresh token has not expired: the backend revoked it, or it was issued by "
        "another backend."
    ) in refused.utils._jwt_reauth_hint()


def test_refused_saved_session_says_to_sign_in_again_and_does_not_read_the_store(refused, monkeypatch):
    from mainsequence import bootstrap as bootstrap_module

    monkeypatch.setattr(bootstrap_module, "_credential_source", "store")
    monkeypatch.setattr(
        config, "saved_session_summary", lambda backend=None: pytest.fail("The store was read")
    )
    monkeypatch.setattr(
        config, "stored_session_available", lambda backend=None: pytest.fail("The store was read")
    )

    hint = refused.utils._jwt_reauth_hint()

    assert hint.endswith(
        " These credentials are your saved session for http://127.0.0.1:8000. "
        "Sign in again with `mainsequence login`."
    )


def test_renewal_refusal_and_backend_failure_read_differently(refused, monkeypatch):
    utils = refused.utils

    class Answer:
        status_code = 401

        def json(self):
            return {}

    monkeypatch.setattr(utils.requests, "post", lambda *args, **kwargs: Answer())
    provider = utils.JWTAuthProvider(
        access_token="copied-access",
        refresh_token=refused.refresh,
        refresh_url="http://127.0.0.1:8000/auth/jwt-token/token/refresh/",
    )

    with pytest.raises(utils.AuthError) as refusal:
        provider.refresh(force=True)
    assert str(refusal.value).startswith(
        "Main Sequence refused to renew the session of this process for "
        "http://127.0.0.1:8000 (HTTP 401). The refresh token expired on "
    )

    Answer.status_code = 503
    with pytest.raises(utils.AuthError) as failure:
        provider.refresh(force=True)
    assert str(failure.value) == (
        "Main Sequence could not renew the session of this process for "
        "http://127.0.0.1:8000: the backend answered HTTP 503. Try again when it is available."
    )


def test_request_reports_a_refused_renewal_with_its_reason(refused):
    utils = refused.utils

    class Answer:
        status_code = 401
        text = '{"detail": "Given token not valid for any token type"}'

    class Session:
        headers: dict = {}

        def get(self, url, **kwargs):
            return Answer()

    class Loaders:
        def refresh_headers(self, force=False, session=None):
            if force:
                raise utils.AuthError("Main Sequence refused to renew the session." + utils._jwt_reauth_hint())
            return {"Authorization": "Bearer copied-access"}

    response = utils.make_request(Session(), "GET", "http://127.0.0.1:8000/api/v1/users/me/", Loaders())

    assert response.status_code == 401
    assert response.code == "auth_error"
    assert response.text.startswith("Main Sequence refused to renew the session.")
    assert "start the process without those two variables" in response.text
    assert "Given token not valid" not in response.text


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
