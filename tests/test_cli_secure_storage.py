"""CLI credential persistence contract."""

from __future__ import annotations

import json

from keyring import errors as keyring_errors

from mainsequence.cli import config


class MemoryKeyring:
    name = "test keyring"
    priority = 1

    def __init__(self) -> None:
        self.values: dict[tuple[str, str], str] = {}

    def get_password(self, service: str, account: str) -> str | None:
        return self.values.get((service, account))

    def set_password(self, service: str, account: str, password: str) -> None:
        self.values[(service, account)] = password

    def delete_password(self, service: str, account: str) -> None:
        try:
            del self.values[(service, account)]
        except KeyError as exc:
            raise keyring_errors.PasswordDeleteError("missing") from exc


def _isolate_auth(monkeypatch, tmp_path, secure_backend) -> None:
    monkeypatch.setattr(config, "AUTH_JSON", tmp_path / "auth.json")
    monkeypatch.setattr(config, "TOKENS_JSON", tmp_path / "token.json")
    monkeypatch.setattr(config, "backend_url", lambda: "https://backend.example")
    monkeypatch.setattr(config, "_secure_keyring_backend", lambda: secure_backend)
    for name in (
        config.ENV_USERNAME,
        config.ENV_ACCESS,
        config.ENV_REFRESH,
        config.LEGACY_ENV_USERNAME,
        config.LEGACY_ENV_ACCESS,
        config.LEGACY_ENV_REFRESH,
        "MAINSEQUENCE_AUTH_MODE",
    ):
        monkeypatch.delenv(name, raising=False)


def test_save_tokens_uses_secure_store_without_auth_json(monkeypatch, tmp_path):
    secure_backend = MemoryKeyring()
    _isolate_auth(monkeypatch, tmp_path, secure_backend)

    assert config.save_tokens("user@example.com", "access", "refresh") is True

    assert not config.AUTH_JSON.exists()
    account = config._keychain_account_for_backend()
    payload = json.loads(secure_backend.get_password(config.KEYCHAIN_SERVICE, account))
    assert payload == {
        "username": "user@example.com",
        "access": "access",
        "refresh": "refresh",
    }


def test_save_tokens_is_process_only_without_secure_store(monkeypatch, tmp_path):
    _isolate_auth(monkeypatch, tmp_path, None)

    assert config.save_tokens("user@example.com", "access", "refresh") is False

    assert config.get_tokens() == {
        "username": "user@example.com",
        "access": "access",
        "refresh": "refresh",
    }
    assert not config.AUTH_JSON.exists()


def test_legacy_auth_json_is_not_used_without_secure_store(monkeypatch, tmp_path):
    _isolate_auth(monkeypatch, tmp_path, None)
    config.write_json(
        config.AUTH_JSON,
        {
            "version": 2,
            "by_backend": {
                "https://backend.example": {
                    "username": "legacy@example.com",
                    "access": "legacy-access",
                    "refresh": "legacy-refresh",
                }
            },
        },
    )

    assert config.get_tokens() == {"username": "", "access": "", "refresh": ""}
    assert config.AUTH_JSON.exists()


def test_legacy_auth_json_migrates_after_secure_readback(monkeypatch, tmp_path):
    secure_backend = MemoryKeyring()
    _isolate_auth(monkeypatch, tmp_path, secure_backend)
    config.write_json(
        config.AUTH_JSON,
        {
            "version": 2,
            "by_backend": {
                "https://backend.example": {
                    "username": "legacy@example.com",
                    "access": "legacy-access",
                    "refresh": "legacy-refresh",
                },
                "https://other.example": {
                    "username": "other@example.com",
                    "access": "other-access",
                    "refresh": "other-refresh",
                },
            },
        },
    )

    assert config.get_tokens() == {
        "username": "legacy@example.com",
        "access": "legacy-access",
        "refresh": "legacy-refresh",
    }
    assert config.read_json(config.AUTH_JSON, {})["by_backend"] == {
        "https://other.example": {
            "username": "other@example.com",
            "access": "other-access",
            "refresh": "other-refresh",
        }
    }


def test_secure_tokens_are_scoped_by_backend(monkeypatch, tmp_path):
    secure_backend = MemoryKeyring()
    _isolate_auth(monkeypatch, tmp_path, secure_backend)

    assert config._write_secure_tokens(
        username="one@example.com",
        access="one-access",
        refresh="one-refresh",
        backend="https://one.example",
    )
    assert config._write_secure_tokens(
        username="two@example.com",
        access="two-access",
        refresh="two-refresh",
        backend="https://two.example",
    )

    assert config._read_secure_tokens("https://one.example")["access"] == "one-access"
    assert config._read_secure_tokens("https://two.example")["access"] == "two-access"


def test_access_only_runtime_token_round_trips(monkeypatch, tmp_path):
    secure_backend = MemoryKeyring()
    _isolate_auth(monkeypatch, tmp_path, secure_backend)
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")

    assert config.save_tokens("", "runtime-access", "") is True
    monkeypatch.delenv(config.ENV_ACCESS)
    monkeypatch.delenv(config.ENV_REFRESH)

    assert config.get_tokens()["access"] == "runtime-access"


def test_access_only_token_is_not_loaded_as_a_jwt_session(monkeypatch, tmp_path):
    secure_backend = MemoryKeyring()
    _isolate_auth(monkeypatch, tmp_path, secure_backend)
    assert config._write_secure_tokens(username="", access="access-only", refresh="")

    assert config.get_tokens() == {"username": "", "access": "", "refresh": ""}


def test_secure_backend_rejects_non_recommended_keyring(monkeypatch):
    class InsecureKeyring:
        priority = 0.5

    limits = []
    monkeypatch.setattr(config.keyring_core, "init_backend", lambda limit: limits.append(limit))
    monkeypatch.setattr(config.keyring, "get_keyring", InsecureKeyring)

    assert config._secure_keyring_backend() is None
    assert limits == [config.keyring_core.recommended]
