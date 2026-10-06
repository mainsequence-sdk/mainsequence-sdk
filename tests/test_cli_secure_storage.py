"""CLI credential persistence contract."""

from __future__ import annotations

import json

from keyring import errors as keyring_errors

from mainsequence.cli import config
from tests._support import jwt_with_expiry as _jwt


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
    raw = secure_backend.get_password(config.KEYCHAIN_SERVICE, account)
    assert raw.isascii()
    assert json.loads(raw) == {
        "v": 1,
        "backend": "https://backend.example",
        "username": "user@example.com",
        "access": "access",
        "refresh": "refresh",
    }


class _RefreshResponse:
    ok = True
    headers = {"content-type": "application/json"}

    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return self._payload


def _saved_session(monkeypatch, tmp_path) -> MemoryKeyring:
    """A session saved by a login, seen from a process that has not read it yet."""
    from mainsequence import bootstrap

    secure_backend = MemoryKeyring()
    _isolate_auth(monkeypatch, tmp_path, secure_backend)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(bootstrap, "_credential_source", None)
    for name in (
        "MAINSEQUENCE_ENDPOINT",
        config.ENV_USERNAME,
        config.ENV_ACCESS,
        config.ENV_REFRESH,
    ):
        # Set first, so the values the code under test exports are undone after the test.
        monkeypatch.setenv(name, "")
    assert config.save_tokens("ada@example.com", "access-1", "refresh-1") is True
    for name in (config.ENV_USERNAME, config.ENV_ACCESS, config.ENV_REFRESH):
        monkeypatch.setenv(name, "")
    return secure_backend


def _saved_record(secure_backend: MemoryKeyring) -> dict:
    account = config._keychain_account_for_backend()
    return json.loads(secure_backend.get_password(config.KEYCHAIN_SERVICE, account))


def test_a_new_process_reports_the_user_of_the_saved_session(monkeypatch, tmp_path):
    from mainsequence import bootstrap

    _saved_session(monkeypatch, tmp_path)

    bootstrap.prime_runtime_env()

    report = config.session_report()
    assert report["source"] == "store"
    assert report["username"] == "ada@example.com"


def test_a_renewal_keeps_the_user_name_of_the_saved_session(monkeypatch, tmp_path):
    from mainsequence import bootstrap
    from mainsequence.cli import api

    secure_backend = _saved_session(monkeypatch, tmp_path)
    bootstrap.prime_runtime_env()
    monkeypatch.setattr(
        api.S, "post", lambda url, data=None: _RefreshResponse({"access": "access-2"})
    )

    assert api.refresh_access() == "access-2"

    assert _saved_record(secure_backend) == {
        "v": 1,
        "backend": "https://backend.example",
        "username": "ada@example.com",
        "access": "access-2",
        "refresh": "refresh-1",
    }


def test_a_renewal_by_a_process_given_only_the_tokens_keeps_the_user_name(monkeypatch, tmp_path):
    from mainsequence.cli import api

    secure_backend = _saved_session(monkeypatch, tmp_path)
    # A launcher passed the saved session's tokens, and not whose they are.
    monkeypatch.setenv(config.ENV_ACCESS, "access-1")
    monkeypatch.setenv(config.ENV_REFRESH, "refresh-1")
    monkeypatch.setattr(
        api.S,
        "post",
        lambda url, data=None: _RefreshResponse({"access": "access-2", "refresh": "refresh-2"}),
    )

    assert api.refresh_access() == "access-2"

    record = _saved_record(secure_backend)
    assert record["username"] == "ada@example.com"
    assert record["refresh"] == "refresh-2"


def test_a_renewal_of_another_session_does_not_take_the_saved_user_name(monkeypatch, tmp_path):
    from mainsequence.cli import api

    secure_backend = _saved_session(monkeypatch, tmp_path)
    monkeypatch.setenv(config.ENV_ACCESS, "other-access")
    monkeypatch.setenv(config.ENV_REFRESH, "other-refresh")
    monkeypatch.setattr(
        api.S, "post", lambda url, data=None: _RefreshResponse({"access": "other-access-2"})
    )

    assert api.refresh_access() == "other-access-2"

    record = _saved_record(secure_backend)
    assert record["refresh"] == "other-refresh"
    assert record["username"] == ""


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


def test_recommended_backend_rejects_non_recommended_keyring(monkeypatch):
    class InsecureKeyring:
        priority = 0.5

    limits = []
    monkeypatch.setattr(config.keyring_core, "init_backend", lambda limit: limits.append(limit))
    monkeypatch.setattr(config.keyring, "get_keyring", InsecureKeyring)

    assert config._recommended_keyring_backend() is None
    assert limits == [config.keyring_core.recommended]


def test_each_system_uses_its_own_store(monkeypatch):
    secret_service, recommended = object(), object()
    monkeypatch.setattr(config, "_secret_service_store", lambda: secret_service)
    monkeypatch.setattr(config, "_recommended_keyring_backend", lambda: recommended)
    monkeypatch.setattr(config._MacOSKeychain, "available", staticmethod(lambda: True))

    monkeypatch.setattr(config.sys, "platform", "darwin")
    assert isinstance(config._secure_keyring_backend(), config._MacOSKeychain)

    monkeypatch.setattr(config.sys, "platform", "linux")
    assert config._secure_keyring_backend() is secret_service

    monkeypatch.setattr(config.sys, "platform", "win32")
    assert config._secure_keyring_backend() is recommended


def test_macos_without_the_security_program_has_no_store(monkeypatch):
    monkeypatch.setattr(config.sys, "platform", "darwin")
    monkeypatch.setattr(config._MacOSKeychain, "available", staticmethod(lambda: False))

    assert config._secure_keyring_backend() is None


def test_refresh_only_record_is_a_session(monkeypatch, tmp_path):
    secure_backend = MemoryKeyring()
    _isolate_auth(monkeypatch, tmp_path, secure_backend)
    assert config._write_secure_tokens(username="user@example.com", access="", refresh="refresh")

    assert config.get_tokens() == {
        "username": "user@example.com",
        "access": "",
        "refresh": "refresh",
    }
    assert config.stored_session_available() is True


def test_record_for_another_backend_is_refused(monkeypatch, tmp_path):
    secure_backend = MemoryKeyring()
    _isolate_auth(monkeypatch, tmp_path, secure_backend)
    secure_backend.set_password(
        config.KEYCHAIN_SERVICE,
        config._keychain_account_for_backend(),
        json.dumps(
            {
                "v": 1,
                "backend": "https://other.example",
                "username": "other@example.com",
                "access": "other-access",
                "refresh": "other-refresh",
            }
        ),
    )

    assert config._read_secure_tokens() == {}
    assert config.stored_session_available() is False


def test_record_written_before_the_backend_field_is_still_read(monkeypatch, tmp_path):
    secure_backend = MemoryKeyring()
    _isolate_auth(monkeypatch, tmp_path, secure_backend)
    secure_backend.set_password(
        config.KEYCHAIN_SERVICE,
        config._keychain_account_for_backend(),
        json.dumps({"username": "old@example.com", "access": "old-access", "refresh": "old-r"}),
    )

    assert config.get_tokens() == {
        "username": "old@example.com",
        "access": "old-access",
        "refresh": "old-r",
    }


class FakeSecretItem:
    def __init__(self, service_store, attributes: dict[str, str], secret: str) -> None:
        self.service_store = service_store
        self.attributes = attributes
        self.secret = secret

    def get_attributes(self) -> dict[str, str]:
        return dict(self.attributes)

    def delete(self) -> None:
        self.service_store.items.remove(self)


class FakeSecretCollection:
    def __init__(self, service_store) -> None:
        self.service_store = service_store
        self.connection = self

    def close(self) -> None:
        self.service_store.closed_connections += 1

    def search_items(self, attributes: dict[str, str]):
        if self.service_store.search_error is not None:
            raise self.service_store.search_error
        for item in list(self.service_store.items):
            if all(item.attributes.get(name) == value for name, value in attributes.items()):
                yield item


class FakeSecretService:
    """Behaves as the keyring library's Secret Service backend: it replaces only its own item."""

    appid = "Python keyring library"

    def __init__(self) -> None:
        self.items: list[FakeSecretItem] = []
        self.closed_connections = 0
        self.search_error: Exception | None = None

    def add(self, service: str, account: str, secret: str, *, own: bool) -> FakeSecretItem:
        attributes = {"service": service, "username": account}
        if own:
            attributes["application"] = self.appid
        item = FakeSecretItem(self, attributes, secret)
        self.items.append(item)
        return item

    def _matches(self, service: str, account: str) -> list[FakeSecretItem]:
        return [
            item
            for item in self.items
            if (item.attributes["service"], item.attributes["username"]) == (service, account)
        ]

    def get_preferred_collection(self) -> FakeSecretCollection:
        return FakeSecretCollection(self)

    def unlock(self, item: FakeSecretItem) -> None:
        pass

    def get_password(self, service: str, account: str) -> str | None:
        matches = self._matches(service, account)
        return matches[0].secret if matches else None

    def set_password(self, service: str, account: str, password: str) -> None:
        for item in self._matches(service, account):
            if item.attributes.get("application") == self.appid:
                item.secret = password
                return
        self.add(service, account, password, own=True)

    def delete_password(self, service: str, account: str) -> None:
        matches = self._matches(service, account)
        if not matches:
            raise keyring_errors.PasswordDeleteError("missing")
        self.items.remove(matches[0])


def test_secret_service_write_replaces_its_own_item_in_place():
    inner = FakeSecretService()
    own = inner.add("svc", "acct", "old", own=True)
    store = config._SecretServiceStore(inner)

    store.set_password("svc", "acct", "new")

    # The same item: another process never finds the record missing during a write.
    assert inner.items == [own]
    assert own.secret == "new"
    assert inner.closed_connections == 1


def test_secret_service_write_removes_the_items_of_other_programs():
    inner = FakeSecretService()
    inner.add("svc", "acct", "written by another program", own=False)
    own = inner.add("svc", "acct", "old", own=True)
    other_account = inner.add("svc", "other", "kept", own=False)
    store = config._SecretServiceStore(inner)

    store.set_password("svc", "acct", "new")

    assert inner.items == [own, other_account]
    assert store.get_password("svc", "acct") == "new"


def test_secret_service_write_without_an_own_item_leaves_one_item():
    inner = FakeSecretService()
    inner.add("svc", "acct", "written by another program", own=False)
    store = config._SecretServiceStore(inner)

    store.set_password("svc", "acct", "new")

    assert [item.secret for item in inner.items] == ["new"]


def test_secret_service_cleanup_failure_is_a_store_error():
    inner = FakeSecretService()
    inner.search_error = RuntimeError("bus lost")
    store = config._SecretServiceStore(inner)

    try:
        store.set_password("svc", "acct", "new")
    except keyring_errors.KeyringError as exc:
        assert "RuntimeError" in str(exc)
    else:
        raise AssertionError("a failing Secret Service call must raise KeyringError")
    assert inner.items == []


def test_secret_service_delete_removes_every_match_and_reports_a_missing_entry():
    inner = FakeSecretService()
    inner.add("svc", "acct", "one", own=True)
    inner.add("svc", "acct", "two", own=False)
    kept = inner.add("svc", "other", "kept", own=True)
    store = config._SecretServiceStore(inner)

    store.delete_password("svc", "acct")

    assert inner.items == [kept]
    try:
        store.delete_password("svc", "acct")
    except keyring_errors.PasswordDeleteError:
        pass
    else:
        raise AssertionError("deleting a missing entry must raise PasswordDeleteError")


class FakeSecurity:
    """Stands in for `/usr/bin/security`: generic passwords with a comment, and a call log."""

    def __init__(self) -> None:
        self.entries: dict[tuple[str, str], str] = {}
        self.comments: dict[tuple[str, str], str] = {}
        self.calls: list[tuple[list[str], str | None]] = []
        self.fail_with: int | None = None
        self.hang = False
        self.add_exit_codes: list[int] = []

    def put(self, service: str, account: str, secret: str, *, ours: bool = True) -> None:
        """An entry this code wrote carries its mark; any other program's entry does not."""
        self.entries[(service, account)] = secret
        if ours:
            self.comments[(service, account)] = config._SECURITY_ENTRY_MARK
        else:
            self.comments.pop((service, account), None)

    def __call__(self, argv, *, input=None, stdin=None, capture_output, text, check, timeout):
        assert argv[0] == config._SECURITY_PROGRAM
        self.calls.append((list(argv[1:]), input))
        if self.hang:
            raise config.subprocess.TimeoutExpired(argv, timeout)
        if self.fail_with is not None:
            return config.subprocess.CompletedProcess(argv, self.fail_with, "", "")
        if argv[1] == "-i":
            exit_code = self.add_exit_codes.pop(0) if self.add_exit_codes else 0
            if exit_code:
                return config.subprocess.CompletedProcess(argv, exit_code, "", "")
            words = input.split()
            assert words[0] == "add-generic-password"
            key = (words[words.index("-s") + 1], words[words.index("-a") + 1])
            self.entries[key] = bytes.fromhex(words[words.index("-X") + 1]).decode("utf-8")
            if "-j" in words:
                self.comments[key] = words[words.index("-j") + 1]
            return config.subprocess.CompletedProcess(argv, 0, "", "")
        key = (argv[argv.index("-s") + 1], argv[argv.index("-a") + 1])
        if key not in self.entries:
            return config.subprocess.CompletedProcess(argv, 44, "", "not found")
        if argv[1] == "delete-generic-password":
            del self.entries[key]
            self.comments.pop(key, None)
            return config.subprocess.CompletedProcess(argv, 0, "", "")
        if "-w" in argv:
            return config.subprocess.CompletedProcess(argv, 0, self.entries[key] + "\n", "")
        comment = f'"{self.comments[key]}"' if key in self.comments else "<NULL>"
        attributes = (
            'keychain: "/Users/someone/Library/Keychains/login.keychain-db"\n'
            'class: "genp"\nattributes:\n'
            f'    "acct"<blob>="{key[1]}"\n    "icmt"<blob>={comment}\n    "svce"<blob>="{key[0]}"\n'
        )
        return config.subprocess.CompletedProcess(argv, 0, attributes, "")

    def commands(self) -> list[str]:
        """Each call by name. A read of the secret is told apart from a look at the attributes."""
        return [
            "read-secret" if "-w" in arguments else arguments[0] for arguments, _stdin in self.calls
        ]


def _fake_security(monkeypatch) -> FakeSecurity:
    fake = FakeSecurity()
    monkeypatch.setattr(config.subprocess, "run", fake)
    # What one process learned about entries must not reach the next test.
    monkeypatch.setattr(config._MacOSKeychain, "_readable", set())
    monkeypatch.setattr(config._MacOSKeychain, "_unreadable", {})
    monkeypatch.setattr(config, "_store_read_error", None)
    return fake


def test_macos_secret_travels_on_standard_input_not_on_a_command_line(monkeypatch):
    fake = _fake_security(monkeypatch)
    record = '{"v": 1, "refresh": "refresh-token-value"}'

    config._MacOSKeychain().set_password("MainSequenceCLI.auth", "default.0123", record)

    assert fake.entries == {("MainSequenceCLI.auth", "default.0123"): record}
    assert fake.comments == {("MainSequenceCLI.auth", "default.0123"): config._SECURITY_ENTRY_MARK}
    for arguments, _stdin in fake.calls:
        assert "refresh-token-value" not in " ".join(arguments)
        assert record.encode().hex() not in " ".join(arguments)
    assert fake.calls[-1][0] == ["-i"]
    assert record.encode().hex() in fake.calls[-1][1]


def test_macos_never_asks_for_the_secret_of_an_entry_it_did_not_write(monkeypatch):
    fake = _fake_security(monkeypatch)
    store = config._MacOSKeychain()
    # What a released version, or the keyring library, leaves: an entry without the mark.
    fake.put("svc", "acct", "written by another program", ours=False)

    for _ in range(3):
        try:
            store.get_password("svc", "acct")
        except keyring_errors.KeyringError as exc:
            assert "another version of the CLI" in str(exc)
        else:
            raise AssertionError("an entry without the mark must not be read")

    # The attributes were looked at once and the secret was never requested: asking
    # `security` for it would show a consent dialog in every process.
    assert fake.commands() == ["find-generic-password"]
    assert "read-secret" not in fake.commands()


def test_macos_write_replaces_an_entry_left_by_another_program(monkeypatch):
    fake = _fake_security(monkeypatch)
    fake.put("svc", "acct", "left by another program", ours=False)

    config._MacOSKeychain().set_password("svc", "acct", "new")

    # Removed first, then added: updating in place would wait for the user's consent.
    assert fake.commands() == ["delete-generic-password", "-i"]
    assert fake.entries == {("svc", "acct"): "new"}
    assert config._MacOSKeychain().get_password("svc", "acct") == "new"


def test_macos_write_updates_in_place_an_entry_this_process_read(monkeypatch):
    fake = _fake_security(monkeypatch)
    fake.put("svc", "acct", "read before")

    assert config._MacOSKeychain().get_password("svc", "acct") == "read before"
    config._MacOSKeychain().set_password("svc", "acct", "new")
    config._MacOSKeychain().set_password("svc", "acct", "newer")

    # Never removed: another process reading at that moment would find no session.
    assert fake.commands() == ["find-generic-password", "read-secret", "-i", "-i"]
    assert fake.entries == {("svc", "acct"): "newer"}


def test_macos_write_removes_and_adds_when_the_update_in_place_fails(monkeypatch):
    fake = _fake_security(monkeypatch)
    fake.put("svc", "acct", "read before")
    store = config._MacOSKeychain()
    assert store.get_password("svc", "acct") == "read before"

    fake.add_exit_codes = [36]
    store.set_password("svc", "acct", "new")

    assert fake.commands()[2:] == ["-i", "delete-generic-password", "-i"]
    assert fake.entries == {("svc", "acct"): "new"}


def test_macos_write_after_a_delete_does_not_update_in_place(monkeypatch):
    fake = _fake_security(monkeypatch)
    fake.put("svc", "acct", "read before")
    store = config._MacOSKeychain()
    assert store.get_password("svc", "acct") == "read before"
    store.delete_password("svc", "acct")
    fake.put("svc", "acct", "left by another program since", ours=False)

    store.set_password("svc", "acct", "new")

    assert fake.commands()[-2:] == ["delete-generic-password", "-i"]
    assert fake.entries == {("svc", "acct"): "new"}


def test_macos_read_handles_a_missing_entry_and_hexadecimal_output(monkeypatch):
    fake = _fake_security(monkeypatch)
    store = config._MacOSKeychain()

    assert store.get_password("svc", "acct") is None

    fake.put("svc", "acct", '{"username": "plain"}')
    assert store.get_password("svc", "acct") == '{"username": "plain"}'

    # `security -w` prints a secret that is not printable ASCII as hexadecimal.
    fake.put("svc", "acct", '{"username": "josé"}'.encode().hex())
    assert store.get_password("svc", "acct") == '{"username": "josé"}'


def test_macos_refuses_a_record_longer_than_one_input_line(monkeypatch):
    fake = _fake_security(monkeypatch)

    try:
        config._MacOSKeychain().set_password("svc", "acct", "x" * 2100)
    except keyring_errors.PasswordSetError:
        pass
    else:
        raise AssertionError("a record that does not fit one line must be refused")

    # Nothing ran: a longer line would have been cut and stored as half a record.
    assert fake.calls == []


def test_macos_refuses_names_that_could_alter_the_command(monkeypatch):
    fake = _fake_security(monkeypatch)

    for service, account in (("svc -A", "acct"), ("svc", "acct\nadd-generic-password")):
        try:
            config._MacOSKeychain().set_password(service, account, "value")
        except keyring_errors.PasswordSetError:
            continue
        raise AssertionError("an unsafe entry name must be refused")

    assert fake.calls == []


def test_macos_failures_are_store_errors_not_missing_entries(monkeypatch):
    fake = _fake_security(monkeypatch)
    store = config._MacOSKeychain()

    fake.fail_with = 36
    for operation in (
        lambda: store.get_password("svc", "acct"),
        lambda: store.delete_password("svc", "acct"),
    ):
        try:
            operation()
        except keyring_errors.PasswordDeleteError as exc:
            raise AssertionError("a failure is not a missing entry") from exc
        except keyring_errors.KeyringError:
            continue
        raise AssertionError("a failing security call must raise KeyringError")

    # A call that waits for the user is cut off and reported the same way.
    fake.fail_with, fake.hang = None, True
    try:
        store.get_password("svc", "other")
    except keyring_errors.KeyringError as exc:
        assert "waiting for the user" in str(exc)
    else:
        raise AssertionError("a security call that does not answer must raise KeyringError")


def test_macos_entry_that_could_not_be_read_is_not_asked_for_again_at_once(monkeypatch):
    fake = _fake_security(monkeypatch)
    store = config._MacOSKeychain()
    fake.hang = True

    for _ in range(3):
        try:
            store.get_password("svc", "acct")
        except keyring_errors.KeyringError as exc:
            assert "waiting for the user" in str(exc)
        else:
            raise AssertionError("an unreadable entry must raise KeyringError")

    # One command reads the session several times; it waits for the user once.
    assert fake.commands() == ["find-generic-password"]

    # A long-lived process asks again once the wait is over, and a readable entry is then used.
    fake.hang = False
    fake.put("svc", "acct", "readable now")
    message, retry_at = config._MacOSKeychain._unreadable[("svc", "acct")]
    assert retry_at > config.time.monotonic()
    config._MacOSKeychain._unreadable[("svc", "acct")] = (message, config.time.monotonic() - 1)
    assert store.get_password("svc", "acct") == "readable now"
    assert config._MacOSKeychain._unreadable == {}


def test_macos_write_replaces_an_entry_that_could_not_be_read(monkeypatch):
    fake = _fake_security(monkeypatch)
    store = config._MacOSKeychain()
    fake.put("svc", "acct", "owned by another program", ours=False)
    try:
        store.get_password("svc", "acct")
    except keyring_errors.KeyringError:
        pass

    # A login: the entry is removed without asking for it, added, and readable at once.
    store.set_password("svc", "acct", "new")

    assert fake.commands() == ["find-generic-password", "delete-generic-password", "-i"]
    assert store.get_password("svc", "acct") == "new"


def test_unreadable_macos_store_means_no_session_not_a_crash(monkeypatch, tmp_path):
    _isolate_auth(monkeypatch, tmp_path, config._MacOSKeychain())
    fake = _fake_security(monkeypatch)
    fake.hang = True

    assert config.get_tokens() == {"username": "", "access": "", "refresh": ""}
    # It is told apart from a machine with no saved session.
    assert "waiting for the user" in config.store_read_error()
    assert "waiting for the user" in config.session_report()["store_error"]

    fake.hang = False
    monkeypatch.setattr(config._MacOSKeychain, "_unreadable", {})
    assert config.get_tokens() == {"username": "", "access": "", "refresh": ""}
    assert config.store_read_error() is None
    assert config.session_report()["store_error"] is None


def test_unreadable_store_is_not_overwritten_from_the_old_plain_file(monkeypatch, tmp_path):
    _isolate_auth(monkeypatch, tmp_path, config._MacOSKeychain())
    fake = _fake_security(monkeypatch)
    entry = (config.KEYCHAIN_SERVICE, config._keychain_account_for_backend())
    fake.put(*entry, "owned by another program", ours=False)
    plain_file = {
        "version": 2,
        "by_backend": {
            "https://backend.example": {
                "username": "old@example.com",
                "access": "old-access",
                "refresh": "old-refresh",
            }
        },
    }
    config.write_json(config.AUTH_JSON, plain_file)

    assert config.get_tokens() == {"username": "", "access": "", "refresh": ""}

    # Nothing was removed or written: the entry and the file are as they were.
    assert fake.commands() == ["find-generic-password"]
    assert fake.entries == {entry: "owned by another program"}
    assert config.read_json(config.AUTH_JSON, {}) == plain_file


def test_macos_round_trip_through_the_store_functions(monkeypatch, tmp_path):
    _isolate_auth(monkeypatch, tmp_path, config._MacOSKeychain())
    fake = _fake_security(monkeypatch)

    assert config.save_tokens("user@example.com", "access", "refresh") is True
    assert list(fake.entries) == [(config.KEYCHAIN_SERVICE, config._keychain_account_for_backend())]
    monkeypatch.delenv(config.ENV_ACCESS)
    monkeypatch.delenv(config.ENV_REFRESH)
    monkeypatch.delenv(config.ENV_USERNAME)

    assert config.get_tokens() == {
        "username": "user@example.com",
        "access": "access",
        "refresh": "refresh",
    }
    assert config.clear_tokens() is True
    assert fake.entries == {}


def test_macos_session_of_another_version_needs_one_login(monkeypatch, tmp_path):
    _isolate_auth(monkeypatch, tmp_path, config._MacOSKeychain())
    fake = _fake_security(monkeypatch)
    entry = (config.KEYCHAIN_SERVICE, config._keychain_account_for_backend())
    released_record = json.dumps(
        {"username": "user@example.com", "access": "released-access", "refresh": "released-r"}
    )
    fake.put(*entry, released_record, ours=False)

    # Not read, and said so: it looks like a machine that is not logged in otherwise.
    assert config.get_tokens() == {"username": "", "access": "", "refresh": ""}
    assert "another version of the CLI" in config.store_read_error()
    assert config.stored_session_available() is False
    assert "read-secret" not in fake.commands()

    # The login replaces it, and the entry is then this code's own.
    assert config.save_tokens("user@example.com", "new-access", "new-refresh") is True
    assert fake.comments == {entry: config._SECURITY_ENTRY_MARK}
    assert config.store_read_error() is None
    monkeypatch.delenv(config.ENV_ACCESS)
    monkeypatch.delenv(config.ENV_REFRESH)
    monkeypatch.delenv(config.ENV_USERNAME)
    assert config.get_tokens()["refresh"] == "new-refresh"
    assert config.clear_tokens() is True


def test_macos_record_updated_by_a_released_version_is_still_read(monkeypatch, tmp_path):
    _isolate_auth(monkeypatch, tmp_path, config._MacOSKeychain())
    fake = _fake_security(monkeypatch)
    entry = (config.KEYCHAIN_SERVICE, config._keychain_account_for_backend())
    assert config.save_tokens("user@example.com", "access", "refresh") is True
    for name in (config.ENV_ACCESS, config.ENV_REFRESH, config.ENV_USERNAME):
        monkeypatch.delenv(name)

    # A released version updates the entry in place: its own record shape, the mark kept.
    fake.entries[entry] = json.dumps(
        {"username": "user@example.com", "access": "released-access", "refresh": "released-r"}
    )

    assert config.get_tokens() == {
        "username": "user@example.com",
        "access": "released-access",
        "refresh": "released-r",
    }
    # The record this version saves keeps the three fields those versions read.
    saved = json.loads(config._token_record(username="u", access="a", refresh="r"))
    assert {name: saved[name] for name in ("username", "access", "refresh")} == {
        "username": "u",
        "access": "a",
        "refresh": "r",
    }
    assert config.clear_tokens() is True


def test_token_expiry_reads_the_claim_and_tolerates_other_values():
    assert config.token_expiry(_jwt(1_900_000_000)) == 1_900_000_000
    assert config.token_expiry(_jwt(None)) is None
    assert config.token_expiry("not-a-jwt") is None
    assert config.token_expiry("") is None
    assert config.token_expiry(None) is None


def test_project_env_credential_keys_names_entries_without_their_values():
    text = (
        "FOO=bar\n"
        "MAINSEQUENCE_REFRESH_TOKEN=secret-refresh\r\n"
        "MAINSEQUENCE_ACCESS_TOKEN=secret-access\n"
        "MAINSEQUENCE_ENDPOINT=https://backend.example\n"
        "# MAINSEQUENCE_TOKEN=commented\n"
    )

    assert config.project_env_credential_keys(text) == [
        "MAINSEQUENCE_ACCESS_TOKEN",
        "MAINSEQUENCE_REFRESH_TOKEN",
    ]
    assert config.project_env_credential_keys("") == []


def test_project_env_credential_keys_reads_the_forms_a_dotenv_loader_accepts():
    text = (
        "export MAINSEQUENCE_ACCESS_TOKEN=secret-access\n"
        "  MAINSEQUENCE_REFRESH_TOKEN = secret-refresh\n"
        "\texport\tMAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET='secret'\n"
        "NOT_MAINSEQUENCE_TOKEN=other\n"
        "MAINSEQUENCE_TOKEN_NOTE=other\n"
    )

    assert config.project_env_credential_keys(text) == [
        "MAINSEQUENCE_ACCESS_TOKEN",
        "MAINSEQUENCE_REFRESH_TOKEN",
        "MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET",
    ]


def test_strip_env_credentials_removes_credentials_and_keeps_every_other_line():
    text = (
        "FOO=bar\r\n"
        "export MAINSEQUENCE_ACCESS_TOKEN=secret-access\r\n"
        "  MAINSEQUENCE_REFRESH_TOKEN = secret-refresh\n"
        "MAINSEQUENCE_AUTH_MODE='runtime_credential'  # written by an old setup\n"
        "MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET=secret\n"
        "MAINSEQUENCE_ENDPOINT=https://backend.example\n"
        "# MAINSEQUENCE_TOKEN=commented\n"
        "export KEEP_ME=1"
    )

    cleaned, removed = config.strip_env_credentials(text)

    # Line endings and the missing final newline are kept as they were.
    assert cleaned == (
        "FOO=bar\r\n"
        "MAINSEQUENCE_ENDPOINT=https://backend.example\n"
        "# MAINSEQUENCE_TOKEN=commented\n"
        "export KEEP_ME=1"
    )
    assert removed == [
        "MAINSEQUENCE_ACCESS_TOKEN",
        "MAINSEQUENCE_REFRESH_TOKEN",
        "MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET",
        "MAINSEQUENCE_AUTH_MODE",
    ]
    assert not any("secret" in name for name in removed)


def test_strip_env_credentials_leaves_a_clean_file_and_a_developer_mode_alone():
    text = (
        "MAINSEQUENCE_AUTH_MODE=jwt\nTAU_LOCAL_MODE=true\nMAINSEQUENCE_ENDPOINT=https://b.example\n"
    )

    assert config.strip_env_credentials(text) == (text, [])
    assert config.strip_env_credentials("") == ("", [])


def test_env_line_key_names_the_assigned_variable_only():
    assert config.env_line_key("FOO=bar") == "FOO"
    assert config.env_line_key("  export FOO = bar") == "FOO"
    assert config.env_line_key("# FOO=bar") is None
    assert config.env_line_key("FOO") is None
    assert config.env_line_key("") is None
    assert config.env_line_key("exportFOO=bar") == "exportFOO"


def test_session_report_judges_the_session_by_its_refresh_token(monkeypatch, tmp_path):
    import time

    _isolate_auth(monkeypatch, tmp_path, MemoryKeyring())
    now = int(time.time())
    monkeypatch.setattr(config, "auth_persistence_label", lambda: "test store")

    assert config.session_report()["authenticated"] is False

    monkeypatch.setenv(config.ENV_ACCESS, _jwt(now - 60))
    monkeypatch.setenv(config.ENV_REFRESH, _jwt(now + 3600))
    report = config.session_report()
    assert report["authenticated"] is True
    assert report["session_expires_at"] == now + 3600
    assert report["access_expires_at"] == now - 60
    assert report["auth_mode"] == "jwt"
    assert not any("signature" in str(value) for value in report.values())

    monkeypatch.setenv(config.ENV_REFRESH, _jwt(now - 1))
    assert config.session_report()["authenticated"] is False


def test_session_report_for_a_runtime_credential_has_no_session_expiry(monkeypatch, tmp_path):
    _isolate_auth(monkeypatch, tmp_path, None)
    monkeypatch.setattr(config, "auth_persistence_label", lambda: "test store")
    token_file = tmp_path / "token"
    token_file.write_text("projected-token", encoding="utf-8")
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "cred-id")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_IDENTITY_TOKEN_FILE", str(token_file))

    report = config.session_report()

    assert report["authenticated"] is True
    assert report["session_expires_at"] is None
    assert report["auth_mode"] == "runtime_credential"
    assert "projected-token" not in json.dumps(report)


def test_session_report_needs_the_runtime_identity_token_file(monkeypatch, tmp_path):
    _isolate_auth(monkeypatch, tmp_path, None)
    monkeypatch.setattr(config, "auth_persistence_label", lambda: "test store")
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "cred-id")
    monkeypatch.delenv("MAINSEQUENCE_RUNTIME_IDENTITY_TOKEN_FILE", raising=False)

    report = config.session_report()

    assert report["authenticated"] is False
    assert report["auth_mode"] == "runtime_credential"
