from uuid import uuid4
import pytest
from pydantic import ValidationError
from mainsequence.client.models_user import User


def payload(**facts):
    return dict(uid=str(uuid4()), username="reader", email="reader@example.test", date_joined="2026-01-01T00:00:00Z",
                is_active=True, api_request_limit=1000, mfa_enabled=False, **facts)


def test_optional_facts_preserve_older_user_responses():
    user = User(**payload())
    assert user.is_organization_admin is None and user.active_team_uids is None


def test_platform_facts_are_typed_without_application_policy():
    team = uuid4()
    user = User(**payload(is_organization_admin=True, active_team_uids=[str(team)]))
    assert user.is_organization_admin is True and user.active_team_uids == [team]
    assert User(**payload(is_organization_admin=False, active_team_uids=[])).active_team_uids == []


@pytest.mark.parametrize("facts", [{"is_organization_admin":"true"}, {"is_organization_admin":1}, {"active_team_uids":["invalid"]}])
def test_malformed_authorization_facts_are_rejected(facts):
    with pytest.raises(ValidationError):
        User(**payload(**facts))
