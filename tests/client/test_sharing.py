import pytest

import mainsequence.client.base as base_mod
import mainsequence.client.models_foundry as models_foundry_mod
import mainsequence.client.models_user as models_user_mod
from tests._support import REPOSITORY_ROOT
from tests.client.support import (
    DemoIdOnlyResource,
    DemoPatchModel,
    DemoShareableModel,
    _class_base_names_from_source,
    _UidRef,
)


def test_shareable_action_posts_user_uid(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"detail":"ok"}'

        @staticmethod
        def json():
            return {"detail": "ok"}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    user_uid = "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"
    response = DemoShareableModel(11).add_to_edit(_UidRef(user_uid), timeout=15)

    assert response == {"detail": "ok"}
    assert captured == {
        "r_type": "POST",
        "url": "https://backend.test/demo-shareable/11/add-to-edit/",
        "payload": {"json": {"user_uid": user_uid}},
        "timeout": 15,
    }


def test_shareable_action_returns_empty_dict_on_no_content(monkeypatch):
    class FakeResponse:
        status_code = 200
        content = b""

    monkeypatch.setattr(base_mod, "make_request", lambda **kwargs: FakeResponse())

    response = DemoShareableModel(9).remove_from_view("8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123")

    assert response == {}


def test_id_only_resource_patch_does_not_route_by_integer_id(monkeypatch):
    def _unexpected_request(**kwargs):
        raise AssertionError("id-only resource should not make a PATCH request")

    monkeypatch.setattr(base_mod, "make_request", _unexpected_request)

    instance = DemoPatchModel(id=9)

    with pytest.raises(ValueError, match="non-empty uid"):
        instance.patch(label="patched")


def test_id_only_resource_delete_does_not_route_by_integer_id(monkeypatch):
    def _unexpected_request(**kwargs):
        raise AssertionError("id-only resource should not make a DELETE request")

    monkeypatch.setattr(base_mod, "make_request", _unexpected_request)

    with pytest.raises(ValueError, match="non-empty uid"):
        DemoIdOnlyResource(9).delete()


def test_id_only_resource_detail_action_does_not_route_by_integer_id(monkeypatch):
    def _unexpected_request(**kwargs):
        raise AssertionError("id-only resource should not make a detail action request")

    monkeypatch.setattr(base_mod, "make_request", _unexpected_request)

    with pytest.raises(ValueError, match="non-empty uid"):
        DemoIdOnlyResource(9).can_view()


def test_shareable_team_action_posts_team_uid(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"detail":"ok"}'

        @staticmethod
        def json():
            return {"detail": "ok"}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    response = DemoShareableModel(11).add_team_to_view(_UidRef(team_uid), timeout=21)

    assert response == {"detail": "ok"}
    assert captured == {
        "r_type": "POST",
        "url": "https://backend.test/demo-shareable/11/add-team-to-view/",
        "payload": {"json": {"team_uid": team_uid}},
        "timeout": 21,
    }


def test_shareable_can_view_parses_permission_state(monkeypatch):
    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "object_uid": "11111111-1111-4111-8111-111111111111",
                "object_type": "tdag.constant",
                "access_level": "view",
                "users": [
                    {
                        "id": 7,
                        "uid": "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123",
                        "first_name": "Jose",
                        "last_name": "Ambrosino",
                        "username": "jose@main-sequence.io",
                        "email": "jose@main-sequence.io",
                        "phone_number": None,
                    }
                ],
                "teams": [],
            }

    monkeypatch.setattr(base_mod, "make_request", lambda **kwargs: FakeResponse())

    access_state = DemoShareableModel(11).can_view()

    assert access_state.object_uid == "11111111-1111-4111-8111-111111111111"
    assert access_state.access_level == "view"
    assert len(access_state.users) == 1
    assert access_state.users[0].uid == "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"
    assert not hasattr(access_state.users[0], "id")
    assert access_state.users[0].email == "jose@main-sequence.io"
    assert access_state.teams == []


def test_shareable_can_edit_parses_permission_state(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "object_uid": "22222222-2222-4222-8222-222222222222",
                "object_type": "tdag.secret",
                "access_level": "edit",
                "users": [
                    {
                        "id": 9,
                        "uid": "9f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123",
                        "first_name": "Ana",
                        "last_name": "Smith",
                        "username": "ana@example.com",
                        "email": "ana@example.com",
                        "phone_number": "+43123456789",
                    }
                ],
                "teams": [
                    {
                        "id": 5,
                        "uid": "3f1cc452-43ec-49cb-b2ba-87dbac164d29",
                        "name": "Research",
                        "description": "Research team",
                        "member_count": 4,
                    }
                ],
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    access_state = DemoShareableModel(15).list_users_can_edit(timeout=20)

    assert access_state.object_uid == "22222222-2222-4222-8222-222222222222"
    assert access_state.access_level == "edit"
    assert len(access_state.users) == 1
    assert access_state.users[0].uid == "9f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"
    assert access_state.teams[0].uid == "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    assert access_state.teams[0].name == "Research"
    assert access_state.teams[0].member_count == 4
    assert captured == {
        "r_type": "GET",
        "url": "https://backend.test/demo-shareable/15/can-edit/",
        "payload": {},
        "timeout": 20,
    }


def test_shareable_access_state_drops_removed_internal_identity():
    removed_field = "_".join(("object", "id"))
    payload = {
        "object_uid": "33333333-3333-4333-8333-333333333333",
        "object_type": "tdag.constant",
        "access_level": "view",
        "users": [],
        "teams": [],
        removed_field: 17,
    }

    state = models_user_mod.ShareableAccessState.model_validate(payload)

    # Tolerant reading (ADR 0032): the retired field is dropped, not declared.
    assert removed_field not in models_user_mod.ShareableAccessState.model_fields
    assert not hasattr(state, removed_field)


@pytest.mark.parametrize(
    ("resource_cls", "accessor_name", "access_level"),
    [
        (models_foundry_mod.Constant, "can_edit", "edit"),
    ],
)
def test_shareable_resources_parse_canonical_public_identity(
    monkeypatch,
    resource_cls,
    accessor_name,
    access_level,
):
    resource_uid = "44444444-4444-4444-8444-444444444444"
    resource = resource_cls.model_construct(uid=resource_uid)
    monkeypatch.setattr(
        resource_cls,
        "_request_detail_action",
        lambda self, **kwargs: {
            "object_uid": resource_uid,
            "object_type": type(self).__name__,
            "access_level": access_level,
            "users": [],
            "teams": [],
        },
    )

    access_state = getattr(resource, accessor_name)()

    assert access_state.object_uid == resource_uid
    assert access_state.access_level == access_level


def test_shareable_access_state_accepts_nullable_canonical_identity():
    access_state = models_user_mod.ShareableAccessState(
        object_uid=None,
        object_type="tdag.constant",
        access_level="view",
    )

    assert access_state.object_uid is None


def test_shareable_models_keep_shareable_object_mixin():
    repo_root = REPOSITORY_ROOT
    models_foundry_bases = _class_base_names_from_source(
        repo_root / "mainsequence" / "client" / "models_foundry.py"
    )
    models_helpers_bases = _class_base_names_from_source(
        repo_root / "mainsequence" / "client" / "models_helpers.py"
    )

    expected = {
        "Artifact": models_foundry_bases,
        "Bucket": models_foundry_bases,
        "CodeRepository": models_foundry_bases,
        "Constant": models_foundry_bases,
        "Secret": models_foundry_bases,
        "ResourceRelease": models_helpers_bases,
    }

    for class_name, source_bases in expected.items():
        assert class_name in source_bases, f"{class_name} class not found"
        assert (
            "ShareableObjectMixin" in source_bases[class_name]
        ), f"{class_name} must inherit ShareableObjectMixin"


def test_team_uses_permission_managed_object_mixin():
    repo_root = REPOSITORY_ROOT
    models_user_bases = _class_base_names_from_source(
        repo_root / "mainsequence" / "client" / "models_user.py"
    )

    assert "Team" in models_user_bases, "Team class not found"
    assert (
        "PermissionManagedObjectMixin" in models_user_bases["Team"]
    ), "Team must inherit PermissionManagedObjectMixin"
    assert (
        "ShareableObjectMixin" not in models_user_bases["Team"]
    ), "Team should not inherit ShareableObjectMixin directly"
