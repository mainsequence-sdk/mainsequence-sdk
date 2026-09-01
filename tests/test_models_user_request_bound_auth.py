from __future__ import annotations

import builtins

import pytest

import mainsequence.client.models_user as models_user_mod

USER_UID = "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"
OTHER_USER_UID = "9f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"


class _FakeResponse:
    def __init__(self, payload: dict):
        self.status_code = 200
        self._payload = payload

    def json(self):
        return self._payload


def _full_user_payload(*, uid: str = USER_UID, username: str = "jose") -> dict:
    return {
        "uid": uid,
        "username": username,
        "email": f"{username}@main-sequence.io",
        "date_joined": "2026-01-01T00:00:00Z",
        "is_active": True,
        "api_request_limit": 10000,
        "mfa_enabled": False,
        "groups": [],
        "user_permissions": [],
        "organization_teams": [],
    }


def _resolve_with_headers(headers: dict[str, str]):
    auth_token = models_user_mod._CURRENT_AUTH_HEADERS.set(headers)
    user_token = models_user_mod._CURRENT_USER.set(None)
    try:
        return models_user_mod.User.get_logged_user()
    finally:
        models_user_mod._CURRENT_USER.reset(user_token)
        models_user_mod._CURRENT_AUTH_HEADERS.reset(auth_token)


def test_get_authenticated_user_details_returns_full_user(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_make_request(**kwargs):
        captured.update(kwargs)
        return _FakeResponse(_full_user_payload())

    monkeypatch.setattr(models_user_mod, "make_request", _fake_make_request)

    user = models_user_mod.User.get_authenticated_user_details()

    assert str(captured["url"]).endswith("/api/v1/users/me/")
    assert user.uid == USER_UID
    assert user.email == "jose@main-sequence.io"


def test_get_logged_user_resolves_trusted_uid_without_backend_lookup(monkeypatch):
    class _FakeSession:
        def get(self, *args, **kwargs):
            raise AssertionError("trusted gateway identity must not trigger a lookup")

    monkeypatch.setattr(
        models_user_mod.User,
        "build_session",
        classmethod(lambda cls: _FakeSession()),
    )

    identity = _resolve_with_headers(
        {
            "X-User-UID": USER_UID.upper(),
            "X-Username": "dashboard-user",
            "X-User-Email": "untrusted@example.com",
        }
    )

    assert identity == models_user_mod.RequestUserIdentity(
        uid=USER_UID,
        username="dashboard-user",
    )
    assert not hasattr(identity, "id")
    assert not hasattr(identity, "email")


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"X-User-UID": "not-a-uuid"},
        {"X-User-ID": "4"},
        {"HTTP_X_USER_ID": "4", "X-User-UID": USER_UID},
        {"Authorization": "Basic credential"},
    ],
)
def test_get_logged_user_rejects_invalid_identity_contract(headers):
    with pytest.raises(models_user_mod.RequestIdentityError):
        _resolve_with_headers(headers)


def test_get_logged_user_bearer_mode_forwards_only_authorization(monkeypatch):
    captured: dict[str, object] = {}

    class _FakeSession:
        def get(self, url, *, headers=None, params=None, timeout=None):
            captured.update(
                url=url,
                headers=headers,
                params=params,
                timeout=timeout,
            )
            return _FakeResponse(_full_user_payload())

    monkeypatch.setattr(
        models_user_mod.User,
        "build_session",
        classmethod(lambda cls: _FakeSession()),
    )

    identity = _resolve_with_headers(
        {
            "Authorization": "Bearer inbound-token",
            "Cookie": "sessionid=must-not-leave-request",
            "Host": "frontend.test",
            "X-User-Email": "untrusted@example.com",
        }
    )

    assert identity.uid == USER_UID
    assert identity.username == "jose"
    assert str(captured["url"]).endswith("/api/v1/users/me/")
    assert captured["headers"] == {"Authorization": "Bearer inbound-token"}
    assert captured["params"] is None


def test_get_logged_user_accepts_matching_bearer_and_gateway_uid(monkeypatch):
    class _FakeSession:
        def get(self, *args, **kwargs):
            return _FakeResponse(_full_user_payload())

    monkeypatch.setattr(
        models_user_mod.User,
        "build_session",
        classmethod(lambda cls: _FakeSession()),
    )

    identity = _resolve_with_headers(
        {"Authorization": "Bearer inbound-token", "X-User-UID": USER_UID}
    )

    assert identity.uid == USER_UID


def test_get_logged_user_rejects_mismatched_bearer_and_gateway_uid(monkeypatch):
    class _FakeSession:
        def get(self, *args, **kwargs):
            return _FakeResponse(_full_user_payload(uid=OTHER_USER_UID))

    monkeypatch.setattr(
        models_user_mod.User,
        "build_session",
        classmethod(lambda cls: _FakeSession()),
    )

    with pytest.raises(models_user_mod.RequestIdentityError, match="does not match"):
        _resolve_with_headers(
            {"Authorization": "Bearer inbound-token", "X-User-UID": USER_UID}
        )


def test_get_logged_user_uses_explicitly_bound_identity_context():
    expected = models_user_mod.RequestUserIdentity(uid=USER_UID, username="jose")
    token = models_user_mod._CURRENT_USER.set(expected)
    try:
        assert models_user_mod.User.get_logged_user() is expected
    finally:
        models_user_mod._CURRENT_USER.reset(token)


def test_get_logged_user_without_bound_context_does_not_import_streamlit(monkeypatch):
    real_import = builtins.__import__

    def _guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "streamlit":
            raise AssertionError("request identity must not import Streamlit")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)

    with pytest.raises(models_user_mod.RequestIdentityError, match="explicitly bound"):
        _resolve_with_headers({})
