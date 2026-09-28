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
