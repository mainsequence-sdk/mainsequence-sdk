from __future__ import annotations

import mainsequence.client.models_user as models_user_mod


class _FakeResponse:
    def __init__(self, payload: dict):
        self.status_code = 200
        self._payload = payload

    def json(self):
        return self._payload


def test_get_authenticated_user_details_accepts_user_details_summary_contract(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_make_request(**kwargs):
        captured.update(kwargs)
        return _FakeResponse(
            {
                "uid": "user-uid-4",
                "username": "jose",
                "email": "jose@main-sequence.io",
                "date_joined": "2026-01-01T00:00:00Z",
                "is_active": True,
                "api_request_limit": 10000,
                "mfa_enabled": False,
                "groups": [
                    {
                        "name": "Organization Admin",
                        "normalized_name": "org_admin",
                    }
                ],
                "organization_teams": [
                    {
                        "uid": "team-uid-1",
                        "name": "Research",
                    }
                ],
            }
        )

    monkeypatch.setattr(models_user_mod, "make_request", _fake_make_request)

    user = models_user_mod.User.get_authenticated_user_details()

    assert str(captured["url"]).endswith("/user/api/user/get_user_details/")
    assert user.groups[0].name == "Organization Admin"
    assert user.groups[0].normalized_name == "org_admin"
    assert not hasattr(user.groups[0], "id")
    assert user.user_permissions == []
    assert user.organization_teams[0].uid == "team-uid-1"

    dumped = user.model_dump()
    assert "user_permissions" not in dumped
    assert "id" not in dumped["groups"][0]
    assert dumped["organization_teams"][0]["uid"] == "team-uid-1"
    assert "id" not in dumped["organization_teams"][0]


def test_user_details_dump_omits_legacy_internal_ids():
    user = models_user_mod.User.model_validate(
        {
            "id": 4,
            "uid": "user-uid-4",
            "username": "jose",
            "email": "jose@main-sequence.io",
            "date_joined": "2026-01-01T00:00:00Z",
            "is_active": True,
            "api_request_limit": 10000,
            "mfa_enabled": False,
            "groups": [
                {
                    "id": 3,
                    "name": "Organization Admin",
                    "normalized_name": "org_admin",
                }
            ],
            "user_permissions": [101, 202],
            "organization_teams": [
                {
                    "id": 9,
                    "uid": "team-uid-1",
                    "name": "Research",
                }
            ],
        }
    )

    dumped = user.model_dump()

    assert user.id == 4
    assert user.user_permissions == [101, 202]
    assert "id" not in dumped
    assert "user_permissions" not in dumped
    assert "id" not in dumped["groups"][0]
    assert dumped["organization_teams"][0]["uid"] == "team-uid-1"
    assert "id" not in dumped["organization_teams"][0]


def test_get_logged_user_uses_request_bound_headers_for_user_lookup(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)

    captured: dict[str, object] = {}

    class _FakeSession:
        def get(self, url, *, headers=None, params=None, timeout=None):
            captured["url"] = url
            captured["headers"] = headers
            captured["params"] = params
            captured["timeout"] = timeout
            return _FakeResponse(
                {
                    "id": 4,
                    "username": "jose",
                    "email": "jose@main-sequence.io",
                    "date_joined": "2026-01-01T00:00:00Z",
                    "is_active": True,
                    "api_request_limit": 10000,
                    "mfa_enabled": False,
                    "groups": [],
                    "user_permissions": [],
                    "organization_teams": [],
                }
            )

    monkeypatch.setattr(
        models_user_mod.User,
        "build_session",
        classmethod(lambda cls: _FakeSession()),
    )
    monkeypatch.setattr(
        models_user_mod.User,
        "get",
        classmethod(lambda cls, *args, **kwargs: (_ for _ in ()).throw(AssertionError("User.get should not be used"))),
    )

    auth_token = models_user_mod._CURRENT_AUTH_HEADERS.set(
        {
            "X-User-ID": "4",
            "Authorization": "Bearer inbound-token",
            "Cookie": "sessionid=abc",
            "Host": "frontend.test",
        }
    )
    user_token = models_user_mod._CURRENT_USER.set(None)
    try:
        user = models_user_mod.User.get_logged_user()
    finally:
        models_user_mod._CURRENT_USER.reset(user_token)
        models_user_mod._CURRENT_AUTH_HEADERS.reset(auth_token)

    assert user.id == 4
    assert str(captured["url"]).endswith("/user/api/user/4/")
    assert captured["params"] == {"serializer": "full"}
    assert captured["headers"]["Authorization"] == "Bearer inbound-token"
    assert captured["headers"]["Cookie"] == "sessionid=abc"
    assert "Host" not in captured["headers"]


def test_get_logged_user_uses_request_bound_uid_header_for_user_lookup(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)

    captured: dict[str, object] = {}

    class _FakeSession:
        def get(self, url, *, headers=None, params=None, timeout=None):
            captured["url"] = url
            captured["headers"] = headers
            captured["params"] = params
            captured["timeout"] = timeout
            return _FakeResponse(
                {
                    "uid": "user-uid-4",
                    "username": "jose",
                    "email": "jose@main-sequence.io",
                    "date_joined": "2026-01-01T00:00:00Z",
                    "is_active": True,
                    "api_request_limit": 10000,
                    "mfa_enabled": False,
                    "groups": [],
                    "user_permissions": [],
                    "organization_teams": [],
                }
            )

    monkeypatch.setattr(
        models_user_mod.User,
        "build_session",
        classmethod(lambda cls: _FakeSession()),
    )

    auth_token = models_user_mod._CURRENT_AUTH_HEADERS.set(
        {
            "X-User-UID": "user-uid-4",
            "Authorization": "Bearer inbound-token",
            "Cookie": "sessionid=abc",
            "Host": "frontend.test",
        }
    )
    user_token = models_user_mod._CURRENT_USER.set(None)
    try:
        user = models_user_mod.User.get_logged_user()
    finally:
        models_user_mod._CURRENT_USER.reset(user_token)
        models_user_mod._CURRENT_AUTH_HEADERS.reset(auth_token)

    assert user.uid == "user-uid-4"
    assert str(captured["url"]).endswith("/user/api/user/get_user_details/")
    assert captured["params"] is None
    assert captured["headers"]["Authorization"] == "Bearer inbound-token"
    assert captured["headers"]["Cookie"] == "sessionid=abc"
    assert "Host" not in captured["headers"]


def test_get_logged_user_returns_header_identity_without_backend_auth(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)

    class _FakeSession:
        def get(self, url, *, headers=None, params=None, timeout=None):
            raise AssertionError("header-only identity should not trigger backend lookup")

    monkeypatch.setattr(
        models_user_mod.User,
        "build_session",
        classmethod(lambda cls: _FakeSession()),
    )

    auth_token = models_user_mod._CURRENT_AUTH_HEADERS.set(
        {
            "X-User-ID": "4",
            "X-Username": "dashboard-user",
            "X-Dashboard-ID": "dashboard-7",
        }
    )
    user_token = models_user_mod._CURRENT_USER.set(None)
    try:
        user = models_user_mod.User.get_logged_user()
    finally:
        models_user_mod._CURRENT_USER.reset(user_token)
        models_user_mod._CURRENT_AUTH_HEADERS.reset(auth_token)

    assert user.id == 4
    assert user.username == "dashboard-user"
    assert user.email == "dashboard-user"
    assert user.date_joined is None
    assert user.api_request_limit is None


def test_get_logged_user_returns_uid_header_identity_without_backend_auth(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)

    class _FakeSession:
        def get(self, url, *, headers=None, params=None, timeout=None):
            raise AssertionError("header-only identity should not trigger backend lookup")

    monkeypatch.setattr(
        models_user_mod.User,
        "build_session",
        classmethod(lambda cls: _FakeSession()),
    )

    auth_token = models_user_mod._CURRENT_AUTH_HEADERS.set(
        {
            "X-User-UID": "user-uid-4",
            "X-Username": "dashboard-user",
            "X-Dashboard-ID": "dashboard-7",
        }
    )
    user_token = models_user_mod._CURRENT_USER.set(None)
    try:
        user = models_user_mod.User.get_logged_user()
    finally:
        models_user_mod._CURRENT_USER.reset(user_token)
        models_user_mod._CURRENT_AUTH_HEADERS.reset(auth_token)

    assert user.uid == "user-uid-4"
    assert user.id is None
    assert user.username == "dashboard-user"
    assert user.email == "dashboard-user"
    assert user.date_joined is None
    assert user.api_request_limit is None


def test_get_logged_user_bearer_fallback_uses_request_bound_headers(monkeypatch):
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)

    captured: dict[str, object] = {}

    class _FakeSession:
        def get(self, url, *, headers=None, params=None, timeout=None):
            captured["url"] = url
            captured["headers"] = headers
            captured["params"] = params
            captured["timeout"] = timeout
            return _FakeResponse(
                {
                    "id": 7,
                    "username": "streamlit-user",
                    "email": "streamlit@main-sequence.io",
                    "date_joined": "2026-01-01T00:00:00Z",
                    "is_active": True,
                    "api_request_limit": 10000,
                    "mfa_enabled": False,
                    "groups": [],
                    "user_permissions": [],
                    "organization_teams": [],
                }
            )

    monkeypatch.setattr(
        models_user_mod.User,
        "build_session",
        classmethod(lambda cls: _FakeSession()),
    )
    monkeypatch.setattr(
        models_user_mod.User,
        "get_authenticated_user_details",
        classmethod(
            lambda cls: (_ for _ in ()).throw(
                AssertionError("get_authenticated_user_details should not be used in request-bound fallback")
            )
        ),
    )

    auth_token = models_user_mod._CURRENT_AUTH_HEADERS.set(
        {
            "Authorization": "Bearer inbound-token",
            "Cookie": "sessionid=abc",
        }
    )
    user_token = models_user_mod._CURRENT_USER.set(None)
    try:
        user = models_user_mod.User.get_logged_user()
    finally:
        models_user_mod._CURRENT_USER.reset(user_token)
        models_user_mod._CURRENT_AUTH_HEADERS.reset(auth_token)

    assert user.id == 7
    assert str(captured["url"]).endswith("/user/api/user/get_user_details/")
    assert captured["params"] is None
    assert captured["headers"]["Authorization"] == "Bearer inbound-token"
    assert captured["headers"]["Cookie"] == "sessionid=abc"
