from __future__ import annotations

import pytest

import mainsequence.client.models_user as models_user_mod

USER_UID = "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"
OTHER_USER_UID = "9f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"
TEAM_UID = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
OTHER_TEAM_UID = "4f1cc452-43ec-49cb-b2ba-87dbac164d29"
NOTIFICATION_UID = "24001fc7-098c-40fa-b398-1d2352b7c224"


def _base_payload(**recipients):
    return models_user_mod.Notification._normalize_notification_payload(
        type="IN",
        title="Deployment complete",
        description="The deployment finished successfully.",
        **recipients,
    )


@pytest.mark.parametrize(
    ("recipients", "expected"),
    [
        ({"target_user_uid": USER_UID}, {"target_user_uid": USER_UID}),
        ({"target_team_uid": TEAM_UID}, {"target_team_uid": TEAM_UID}),
        (
            {"user_uids": [USER_UID, OTHER_USER_UID]},
            {"user_uids": [USER_UID, OTHER_USER_UID]},
        ),
        (
            {"team_uids": [TEAM_UID, OTHER_TEAM_UID]},
            {"team_uids": [TEAM_UID, OTHER_TEAM_UID]},
        ),
    ],
)
def test_notification_payload_uses_public_recipient_uids(recipients, expected):
    payload = _base_payload(**recipients)

    for key, value in expected.items():
        assert payload[key] == value
    assert "target_user" not in payload
    assert "target_team" not in payload
    assert "user_ids" not in payload
    assert "team_ids" not in payload


def test_notification_payload_rejects_non_uuid_recipient():
    with pytest.raises(ValueError, match="valid UUID"):
        _base_payload(target_user_uid="42")


def test_notification_response_exposes_only_public_relationship_uids():
    notification = models_user_mod.Notification.model_validate(
        {
            "id": 101,
            "uid": NOTIFICATION_UID,
            "source": "organization",
            "created_by_user": 4,
            "created_by_user_uid": USER_UID,
            "source_organization": 2,
            "source_organization_uid": "00000000-0000-4000-8000-000000000002",
            "type": "IN",
            "created_at": "2026-08-17T12:00:00Z",
            "title": "Deployment complete",
            "description": "The deployment finished successfully.",
            "meta_data": {},
            "is_global": False,
            "target_user": 7,
            "target_user_uid": OTHER_USER_UID,
            "target_team": None,
            "target_team_uid": None,
            "include_email": False,
            "is_read": False,
        }
    )

    assert notification.uid == NOTIFICATION_UID
    assert notification.created_by_user_uid == USER_UID
    assert notification.target_user_uid == OTHER_USER_UID
    assert not hasattr(notification, "id")
    assert not hasattr(notification, "created_by_user")
    assert not hasattr(notification, "target_user")
