from unittest.mock import Mock

import pytest

from mainsequence._request_identity import _request_scope
from mainsequence.client import RequestIdentityError, RequestUserIdentity, User

USER_UID = "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"


def test_getter_requires_authenticated_request():
    with pytest.raises(RequestIdentityError):
        User.get_logged_user()
    with _request_scope():
        with pytest.raises(RequestIdentityError):
            User.get_logged_user()


def test_getter_uses_one_identity_and_resets():
    expected = RequestUserIdentity(uid=USER_UID)
    with _request_scope() as context:
        context.user = expected
        assert User.get_logged_user() is expected
        assert User.get_logged_user() is expected
    with pytest.raises(RequestIdentityError):
        User.get_logged_user()


def test_process_profile_still_uses_process_session(monkeypatch):
    import mainsequence.client.models_user as models

    response = Mock(status_code=200)
    response.json.return_value = {
        "uid": USER_UID,
        "username": "process",
        "email": "process@example.test",
        "date_joined": "2026-01-01T00:00:00Z",
        "is_active": True,
        "api_request_limit": 10000,
        "mfa_enabled": False,
        "groups": [],
        "user_permissions": [],
        "organization_teams": [],
    }
    request = Mock(return_value=response)
    monkeypatch.setattr(models, "make_request", request)
    assert User.get_authenticated_user_details().uid == USER_UID
    assert request.call_args.kwargs["url"].endswith("/api/v1/users/me/")


# A requester-bound request: the caller is the acting application, and the
# request scope also holds the person it works for (ADR-0036).
REQUESTER_UID = "5b0f9a8e-3c2d-4e1f-9a7b-6c5d4e3f2a1b"
TEAM_UID = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"


def test_requester_getter_requires_authenticated_request():
    with pytest.raises(RequestIdentityError, match=r"User\.get_requester\(\)"):
        User.get_requester()
    with _request_scope() as context:  # a public route: no caller
        context.requester = RequestUserIdentity(uid=REQUESTER_UID)
        with pytest.raises(RequestIdentityError):
            User.get_requester()
    with pytest.raises(RequestIdentityError, match=r"User\.get_logged_user\(\)"):
        User.get_logged_user()


def test_requester_getter_returns_the_requester_beside_the_unchanged_caller():
    caller = RequestUserIdentity(uid=USER_UID)
    requester = RequestUserIdentity(
        uid=REQUESTER_UID, team_uids=[TEAM_UID], is_organization_admin=True
    )
    with _request_scope() as context:
        context.user = caller
        assert User.get_requester() is None
        context.requester = requester
        assert User.get_requester() is requester
        assert User.get_requester().team_uids == (TEAM_UID,)
        # The person's own admin flag, never the caller's.
        assert User.get_requester().is_organization_admin is True
        assert User.get_logged_user().is_organization_admin is False
        assert User.get_logged_user() is caller
    assert context.requester is None
    with pytest.raises(RequestIdentityError):
        User.get_requester()


def test_requester_getter_rejects_an_invalid_context_value():
    with _request_scope() as context:
        context.user = RequestUserIdentity(uid=USER_UID)
        context.requester = REQUESTER_UID
        with pytest.raises(RequestIdentityError, match="Invalid request requester"):
            User.get_requester()
